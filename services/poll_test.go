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

type SpyStateRepository struct {
	models.StateRepository
	checkpoint time.Time
}

func (f *SpyStateRepository) GetCheckpoint(CheckpointType models.CheckpointType) (time.Time, error) {
	return f.checkpoint, nil
}

func (f *SpyStateRepository) UpdateCheckpoint(checkpointType models.CheckpointType, time time.Time) (bool, error) {
	f.checkpoint = time
	return true, nil
}

type FakeReadyPublisher struct {
	messages    chan any
	numAttempts int
	errorOn     int
}

func (f *FakeReadyPublisher) SendMessage(ctx context.Context, event any) (string, error) {
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
		readyPublisher            *FakeReadyPublisher
		expectedCheckpoints       []time.Time
		expectedCurrentCheckpoint time.Time
	}{
		"Can poll": {
			readyPublisher:            &FakeReadyPublisher{messages: make(chan any, 4)},
			expectedCheckpoints:       []time.Time{originalCheckpoint, originalCheckpoint.Add(time.Minute * 2)},
			expectedCurrentCheckpoint: originalCheckpoint.Add(time.Minute * time.Duration(4)),
		},
		"Can poll when publishing initially failed": {
			readyPublisher:            &FakeReadyPublisher{messages: make(chan any, 4), errorOn: 1},
			expectedCheckpoints:       []time.Time{originalCheckpoint, originalCheckpoint.Add(time.Minute).Add(-time.Millisecond), originalCheckpoint.Add(-time.Millisecond).Add(time.Minute * 3)},
			expectedCurrentCheckpoint: originalCheckpoint.Add(-time.Millisecond).Add(time.Minute * 5),
		},
		"Can poll when publishing failed on middle request": {
			readyPublisher:            &FakeReadyPublisher{messages: make(chan any, 4), errorOn: 2},
			expectedCheckpoints:       []time.Time{originalCheckpoint, originalCheckpoint.Add(time.Minute), originalCheckpoint.Add(time.Minute * 3)},
			expectedCurrentCheckpoint: originalCheckpoint.Add(time.Minute * 5),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			anchorRepo := &SpyAnchorRepository{}
			stateRepo := &SpyStateRepository{checkpoint: originalCheckpoint}

			rp := NewRequestPoller(anchorRepo, stateRepo, test.readyPublisher)

			// T0 request poller starts
			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				rp.Run(ctx)
			}()

			// a. publisher attempts to publish request. If the publisher errors (indicated by errorOn field set), it only sends requests that were successful.
			//	 0-2 messages could have been sent
			// b. sleep for 1s
			// c. publisher publishes 2 requests
			// repeat b + c until 4 messages are published in total
			// cancel which gracefully shutdowns the poller before the next run, wait for graceful shutdown to complete
			waitForMesssages(test.readyPublisher.messages, 4)
			cancel()
			wg.Wait()

			// the poller should have ran 2-3 times. 3 times if there was an error because one of the runs published <2 messages
			if !reflect.DeepEqual(anchorRepo.receivedCheckpoints, test.expectedCheckpoints) {
				t.Errorf("incorrect checkpoints used: expected %v, got %v", test.expectedCheckpoints, anchorRepo.receivedCheckpoints)
			}

			// the most recent checkpoint should be the CreatedAt time of the last received message
			if stateRepo.checkpoint != test.expectedCurrentCheckpoint {
				t.Errorf("incorrect current checkpoint: expected %v, got %v", test.expectedCurrentCheckpoint, stateRepo.checkpoint)
			}
		})
	}

}
