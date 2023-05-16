package services

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ceramicnetwork/go-cas/models"
)

var originalCheckpoint = time.Now().Add(-time.Hour)

type SpyAnchorRepository struct {
	models.AnchorRepository
	receivedCheckpoints []time.Time
}

func (f *SpyAnchorRepository) GetRequests(status models.RequestStatus, since time.Time, limit int) ([]*models.AnchorRequestMessage, error) {
	f.receivedCheckpoints = append(f.receivedCheckpoints, since)

	return []*models.AnchorRequestMessage{
		{CreatedAt: since.Add(time.Minute * 1)},
		{CreatedAt: since.Add(time.Minute * 2)},
	}, nil
}

type StubStateRepository struct {
	models.StateRepository
	checkpoint time.Time
}

func (f *StubStateRepository) GetCheckpoint(CheckpointType models.CheckpointType) (time.Time, error) {
	return f.checkpoint, nil
}

func (f *StubStateRepository) UpdateCheckpoint(checkpointType models.CheckpointType, time time.Time) (bool, error) {
	f.checkpoint = time
	return true, nil
}

type FakeQueuePublisher struct {
	messages    chan any
	numAttempts int
	errorOn     int
}

func (f *FakeQueuePublisher) SendMessage(ctx context.Context, event any) (string, error) {
	select {
	case <-ctx.Done():
		return "", errors.New("context cancelled")
	default:
		f.numAttempts = f.numAttempts + 1
		if f.numAttempts == f.errorOn {
			return "", errors.New("TestError")
		}
		f.messages <- event
		return "msgId", nil
	}

}

func waitForMesssages(messageChannel chan any, n int) []any {
	messages := make([]any, n)
	for i := 0; i < n; i++ {
		message := <-messageChannel
		messages[i] = message
	}
	return messages
}

// if no requests do not publish anything and try again after a second

func TestPoller(t *testing.T) {
	tests := map[string]struct {
		queuePublisher            *FakeQueuePublisher
		expectedCheckpoints       []time.Time
		expectedCurrentCheckpoint time.Time
	}{
		"Can poll": {
			queuePublisher:            &FakeQueuePublisher{messages: make(chan any, 4)},
			expectedCheckpoints:       []time.Time{originalCheckpoint, originalCheckpoint.Add(time.Minute * 2)},
			expectedCurrentCheckpoint: originalCheckpoint.Add(time.Minute * time.Duration(4)),
		},
		"Can poll when publishing initially failed": {
			queuePublisher:            &FakeQueuePublisher{messages: make(chan any, 4), errorOn: 1},
			expectedCheckpoints:       []time.Time{originalCheckpoint, originalCheckpoint.Add(time.Minute).Add(-time.Millisecond), originalCheckpoint.Add(-time.Millisecond).Add(time.Minute * 3)},
			expectedCurrentCheckpoint: originalCheckpoint.Add(-time.Millisecond).Add(time.Minute * 5),
		},
		"Can poll when publishing failed on middle request": {
			queuePublisher:            &FakeQueuePublisher{messages: make(chan any, 4), errorOn: 2},
			expectedCheckpoints:       []time.Time{originalCheckpoint, originalCheckpoint.Add(time.Minute), originalCheckpoint.Add(time.Minute * 3)},
			expectedCurrentCheckpoint: originalCheckpoint.Add(time.Minute * 5),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			anchorRepo := &SpyAnchorRepository{}
			stateRepo := &StubStateRepository{checkpoint: originalCheckpoint}

			rp := NewRequestPoller(anchorRepo, stateRepo, test.queuePublisher)

			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				rp.Run(ctx)
			}()

			waitForMesssages(test.queuePublisher.messages, 4)
			cancel()
			wg.Wait()

			if !reflect.DeepEqual(anchorRepo.receivedCheckpoints, test.expectedCheckpoints) {
				t.Errorf("incorrect checkpoints used: expected %v, got %v", test.expectedCheckpoints, anchorRepo.receivedCheckpoints)
			}

			if stateRepo.checkpoint != test.expectedCurrentCheckpoint {
				t.Errorf("incorrect current checkpoint: expected %v, got %v", test.expectedCurrentCheckpoint, stateRepo.checkpoint)
			}
		})
	}

}
