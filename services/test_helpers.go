package services

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	iface "github.com/ipfs/boxo/coreiface"
	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"

	"github.com/3box/pipeline-tools/cd/manager/common/job"

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

func (m *MockAnchorRepository) GetRequests(_ context.Context, _ models.RequestStatus, newerThan time.Time, _ time.Time, _ int) ([]*models.AnchorRequest, error) {
	m.receivedCheckpoints = append(m.receivedCheckpoints, newerThan)

	return []*models.AnchorRequest{
		{CreatedAt: newerThan.Add(time.Minute * 1)},
		{CreatedAt: newerThan.Add(time.Minute * 2)},
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

func (m *MockStateRepository) GetCheckpoint(_ context.Context, CheckpointType models.CheckpointType) (time.Time, error) {
	return m.checkpoint, nil
}

func (m *MockStateRepository) UpdateCheckpoint(_ context.Context, checkpointType models.CheckpointType, time time.Time) (bool, error) {
	m.checkpoint = time
	return true, nil
}

func (m *MockStateRepository) StoreCid(_ context.Context, streamCid *models.StreamCid) (bool, error) {
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

func (m *MockStateRepository) UpdateTip(_ context.Context, newTip *models.StreamTip) (bool, *models.StreamTip, error) {
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
	jobStore  map[string]*job.JobState
	failCount int
}

func (m *MockJobRepository) CreateJob(_ context.Context) (string, error) {
	if m.jobStore == nil {
		m.jobStore = make(map[string]*job.JobState, 1)
	}
	if m.failCount > 0 {
		m.failCount--
		return "", fmt.Errorf("failed to create job")
	}
	newJob := models.NewJob(job.JobType_Anchor, nil)
	m.jobStore[newJob.JobId] = &newJob
	return newJob.JobId, nil
}

func (m *MockJobRepository) QueryJob(_ context.Context, id string) (*job.JobState, error) {
	if jobState, found := m.jobStore[id]; found {
		return jobState, nil
	}
	return nil, fmt.Errorf("job %s not found", id)
}

func (m *MockJobRepository) finishJobs(count int) {
	for _, js := range m.jobStore {
		if count > 0 {
			js.Stage = job.JobStage_Completed
			count--
		}
	}
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

func (m *MockPublisher) GetUrl() string {
	return ""
}

type MockBatchPublisher struct {
	messages chan any
	fail     bool
}

func (m *MockBatchPublisher) SendMessage(_ context.Context, event any) (string, error) {
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
	failCount   int
}

func (m *MockQueueMonitor) GetUtilization(_ context.Context) (int, int, error) {
	if m.failCount > 0 {
		m.failCount--
		return 0, 0, fmt.Errorf("failed to get utilization")
	}
	return m.unprocessed, 0, nil
}

type MockMetricService struct {
	mx            sync.RWMutex
	counts        map[models.MetricName]int
	distributions map[models.MetricName]int
}

func (m *MockMetricService) Count(ctx context.Context, name models.MetricName, val int) error {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.counts == nil {
		m.counts = make(map[models.MetricName]int)
	}
	m.counts[name] = m.counts[name] + val
	return nil
}

func (m *MockMetricService) Distribution(ctx context.Context, name models.MetricName, val int) error {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.distributions == nil {
		m.distributions = make(map[models.MetricName]int)
	}
	m.distributions[name] = val
	return nil
}

func (m *MockMetricService) QueueGauge(ctx context.Context, name string, monitor models.QueueMonitor) error {
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

type MockNotifier struct{}

func (n MockNotifier) SendAlert(string, string, string) error { return nil }

type MockPubsubApi struct {
	iface.PubSubAPI
	publishedMessages []models.IpfsPubsubPublishMessage
}

func (p *MockPubsubApi) Publish(ctx context.Context, topic string, data []byte) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("timed out while publishing")
	default:
		p.publishedMessages = append(p.publishedMessages, models.IpfsPubsubPublishMessage{CreatedAt: time.Now(), Topic: topic, Data: data})
		return nil
	}
}

type MockIpfsCoreApi struct {
	iface.CoreAPI
	pubsubApi MockPubsubApi
}

func NewMockIpfsCoreApi(node *core.IpfsNode) *MockIpfsCoreApi {
	api, _ := coreapi.NewCoreAPI(node)

	return &MockIpfsCoreApi{CoreAPI: api, pubsubApi: MockPubsubApi{PubSubAPI: api.PubSub()}}
}

func (i *MockIpfsCoreApi) PubSub() iface.PubSubAPI {
	return &i.pubsubApi
}
