package services

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ceramicnetwork/go-cas/models"
)

type MockAnchorRepository struct {
	receivedCheckpoints []time.Time
	requestStore        map[string]statusUpdate
	shouldFailUpdate    bool
}

type statusUpdate struct {
	newStatus             models.RequestStatus
	allowedSourceStatuses []models.RequestStatus
}

func (m *MockAnchorRepository) GetRequests(_ context.Context, _ models.RequestStatus, since time.Time, _ int) ([]*models.AnchorRequest, error) {
	m.receivedCheckpoints = append(m.receivedCheckpoints, since)

	return []*models.AnchorRequest{
		{CreatedAt: since.Add(time.Minute * 1)},
		{CreatedAt: since.Add(time.Minute * 2)},
	}, nil
}

func (m *MockAnchorRepository) UpdateStatus(_ context.Context, id uuid.UUID, status models.RequestStatus, allowedSourceStatuses []models.RequestStatus) error {
	if m.shouldFailUpdate {
		return fmt.Errorf("failed to update status")
	}
	if m.requestStore == nil {
		m.requestStore = make(map[string]statusUpdate, 1)
	}
	m.requestStore[id.String()] = statusUpdate{status, allowedSourceStatuses}
	return nil
}

func (m *MockAnchorRepository) getNumUpdates() int {
	return len(m.requestStore)
}

func (m *MockAnchorRepository) getStatusUpdate(id uuid.UUID) statusUpdate {
	return m.requestStore[id.String()]
}

type MockStateRepository struct {
	checkpoint time.Time
	tipStore   map[string]*models.StreamTip
	cidStore   map[string]bool
}

func (m *MockStateRepository) GetCheckpoint(CheckpointType models.CheckpointType) (time.Time, error) {
	return m.checkpoint, nil
}

func (m *MockStateRepository) UpdateCheckpoint(checkpointType models.CheckpointType, time time.Time) (bool, error) {
	m.checkpoint = time
	return true, nil
}

func (m *MockStateRepository) StoreCid(streamCid *models.StreamCid) (bool, error) {
	if m.cidStore == nil {
		m.cidStore = make(map[string]bool, 1)
	}
	key := fmt.Sprintf("%s_%s", streamCid.StreamId, streamCid.Cid)
	val := m.cidStore[key]
	if !val {
		m.cidStore[key] = true
		return true, nil
	}
	return false, nil
}

func (m *MockStateRepository) UpdateTip(newTip *models.StreamTip) (bool, *models.StreamTip, error) {
	if m.tipStore == nil {
		m.tipStore = make(map[string]*models.StreamTip, 1)
	}
	key := fmt.Sprintf("%s_%s", newTip.StreamId, newTip.Origin)
	var oldTip *models.StreamTip = nil
	var found bool
	if oldTip, found = m.tipStore[key]; found {
		if newTip.Timestamp.Before(oldTip.Timestamp) {
			return false, nil, nil
		}
	}
	m.tipStore[key] = newTip
	return true, oldTip, nil
}

type MockJobRepository struct {
	jobStore  map[string]bool
	failCount int
}

func (m *MockJobRepository) CreateJob() error {
	if m.jobStore == nil {
		m.jobStore = make(map[string]bool, 1)
	}
	if m.failCount > 0 {
		m.failCount--
		return fmt.Errorf("failed to create job")
	}
	m.jobStore[uuid.New().String()] = true
	return nil
}

type MockPublisher struct {
	messages    chan any
	numAttempts int
	errorOn     int
}

func (m *MockPublisher) SendMessage(ctx context.Context, event any) (string, error) {
	select {
	case <-ctx.Done():
		return "", errors.New("context cancelled")
	default:
		m.numAttempts = m.numAttempts + 1
		if m.numAttempts == m.errorOn {
			return "", errors.New("TestError")
		}
		m.messages <- event
		return "msgId", nil
	}

}

type MockBatchPublisher struct {
	messages chan any
	fail     bool
}

func (m *MockBatchPublisher) SendMessage(ctx context.Context, event any) (string, error) {
	if m.fail {
		return "", errors.New("test error")
	}
	m.messages <- event
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

type MockQueueMonitor struct {
	unprocessed int
	inFlight    int
	jobDb       *MockJobRepository
	failCount   int
}

func (m *MockQueueMonitor) GetQueueUtilization(ctx context.Context) (int, int, error) {
	if m.failCount > 0 {
		m.failCount--
		return 0, 0, fmt.Errorf("failed to get utilization")
	}
	// Increment in flight by as many jobs as were created in the job DB and decrement unprocessed by the same number
	return m.unprocessed - len(m.jobDb.jobStore), m.inFlight + len(m.jobDb.jobStore), nil
}

type MockMetricService struct {
	counts map[models.MetricName]int
}

func (m *MockMetricService) Count(ctx context.Context, name models.MetricName, val int) error {
	if m.counts == nil {
		m.counts = make(map[models.MetricName]int)
	}
	m.counts[name] = m.counts[name] + val
	return nil
}

func (m *MockMetricService) Shutdown(ctx context.Context) {
}

func Assert(t *testing.T, expected any, received any, message string) {
	if expected != received {
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(message+": expected %v, received %v", expected, received)
		}
	}
}
