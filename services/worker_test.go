package services

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func TestLaunch(t *testing.T) {
	type testConfig struct {
		monitor         *FakeQueueMonitor
		jobDb           *FakeJobRepository
		maxWorkers      int
		expectedWorkers int
	}
	tests := make(map[string]testConfig, 0)
	fakeRepo := &FakeJobRepository{failCount: 0}
	tests["create as many jobs as unprocessed batches"] = testConfig{
		monitor:         &FakeQueueMonitor{5, 0, fakeRepo, 0},
		jobDb:           fakeRepo,
		maxWorkers:      -1,
		expectedWorkers: 5,
	}
	fakeRepo = &FakeJobRepository{failCount: 0}
	tests["create jobs upto configured max"] = testConfig{
		monitor:         &FakeQueueMonitor{3, 0, fakeRepo, 0},
		jobDb:           fakeRepo,
		maxWorkers:      2,
		expectedWorkers: 2,
	}
	fakeRepo = &FakeJobRepository{failCount: 0}
	tests["do not create jobs if max already in flight"] = testConfig{
		monitor:         &FakeQueueMonitor{3, 2, fakeRepo, 0},
		jobDb:           fakeRepo,
		maxWorkers:      2,
		expectedWorkers: 0,
	}
	fakeRepo = &FakeJobRepository{failCount: 0}
	tests["do not create jobs if no unprocessed batches"] = testConfig{
		monitor:         &FakeQueueMonitor{0, 0, fakeRepo, 0},
		jobDb:           fakeRepo,
		maxWorkers:      -1,
		expectedWorkers: 0,
	}
	fakeRepo = &FakeJobRepository{failCount: 0}
	tests["create only as many jobs as necessary"] = testConfig{
		monitor:         &FakeQueueMonitor{1, 0, fakeRepo, 0},
		jobDb:           fakeRepo,
		maxWorkers:      2,
		expectedWorkers: 1,
	}
	fakeRepo = &FakeJobRepository{failCount: 1}
	tests["continue creating jobs after db error"] = testConfig{
		monitor:         &FakeQueueMonitor{3, 0, fakeRepo, 0},
		jobDb:           fakeRepo,
		maxWorkers:      3,
		expectedWorkers: 3,
	}
	fakeRepo = &FakeJobRepository{failCount: 0}
	tests["continue creating jobs after queue error"] = testConfig{
		monitor:         &FakeQueueMonitor{3, 0, fakeRepo, 1},
		jobDb:           fakeRepo,
		maxWorkers:      3,
		expectedWorkers: 3,
	}
	t.Setenv("ANCHOR_BATCH_MONITOR_TICK", "100ms")
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("MAX_ANCHOR_WORKERS", strconv.FormatInt(int64(test.maxWorkers), 10))
			t.Logf("start: unprocessed=%d, inflight=%d, max=%d", test.monitor.unprocessed, test.monitor.inFlight, test.maxWorkers)
			workerService := NewWorkerService(test.monitor, test.jobDb)
			// Cancel the context after at least 3 iterations of the ticker so that we can test how the service behaves
			// over time, especially when there are errors.
			ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
			workerService.Launch(ctx)
			cancel()
			if len(test.jobDb.jobStore) != test.expectedWorkers {
				t.Errorf("incorrect number %d of workers created, expected %d", len(test.jobDb.jobStore), test.expectedWorkers)
			}
			numProcessed, numInFlight, _ := test.monitor.GetQueueUtilization(nil)
			t.Logf("end: unprocessed=%d, inflight=%d, created=%d", numProcessed, numInFlight, len(test.jobDb.jobStore))
		})
	}
}
