package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/ceramicnetwork/go-cas/models"
)

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

type FakeStateRepository struct {
	models.StateRepository
	checkpoint time.Time
	cidStore   map[string]bool
}

func (f *FakeStateRepository) GetCheckpoint(CheckpointType models.CheckpointType) (time.Time, error) {
	return f.checkpoint, nil
}

func (f *FakeStateRepository) UpdateCheckpoint(checkpointType models.CheckpointType, time time.Time) (bool, error) {
	f.checkpoint = time
	return true, nil
}

func (f *FakeStateRepository) StoreCid(streamCid *models.StreamCid) (bool, error) {
	key := fmt.Sprint(streamCid)
	val := f.cidStore[key]

	if !val {
		f.cidStore[key] = true
		return true, nil
	}

	return false, nil
}

type FakeJobRepository struct {
	models.JobRepository
	jobStore map[string]bool
}

func (f *FakeJobRepository) CreateJob() error {
	jobId := uuid.New().String()
	f.jobStore[jobId] = true
	return nil
}

type FakePublisher struct {
	messages    chan any
	numAttempts int
	errorOn     int
}

func (f *FakePublisher) SendMessage(ctx context.Context, event any) (string, error) {
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

type FakeBatchPublisher struct {
	messages chan any
	fail     bool
}

func (f *FakeBatchPublisher) SendMessage(ctx context.Context, event any) (string, error) {
	if f.fail {
		return "", errors.New("test error")
	}
	f.messages <- event
	return "msgId", nil

}

func waitForMesssages(messageChannel chan any, n int) []any {
	messages := make([]any, n)
	for i := 0; i < n; i++ {
		message := <-messageChannel
		messages[i] = message
	}
	return messages
}
