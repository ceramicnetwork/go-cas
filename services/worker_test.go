package services

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	tests := map[string]struct {
		monitor         *MockQueueMonitor
		jobDb           *MockJobRepository
		maxWorkers      int
		expectedWorkers int
	}{
		"create as many jobs as unprocessed batches": {
			monitor:         &MockQueueMonitor{5, 0, &MockJobRepository{failCount: 0}, 0},
			maxWorkers:      -1,
			expectedWorkers: 5,
		},
		"create jobs upto configured max": {
			monitor:         &MockQueueMonitor{3, 0, &MockJobRepository{failCount: 0}, 0},
			maxWorkers:      2,
			expectedWorkers: 2,
		},
		"do not create jobs if max already in flight": {
			monitor:         &MockQueueMonitor{3, 2, &MockJobRepository{failCount: 0}, 0},
			maxWorkers:      2,
			expectedWorkers: 0,
		},
		"do not create jobs if no unprocessed batches": {
			monitor:         &MockQueueMonitor{0, 0, &MockJobRepository{failCount: 0}, 0},
			maxWorkers:      -1,
			expectedWorkers: 0,
		},
		"create only as many jobs as necessary": {
			monitor:         &MockQueueMonitor{1, 0, &MockJobRepository{failCount: 0}, 0},
			maxWorkers:      2,
			expectedWorkers: 1,
		},
		"continue creating jobs after db error": {
			monitor:         &MockQueueMonitor{3, 0, &MockJobRepository{failCount: 1}, 0},
			maxWorkers:      3,
			expectedWorkers: 3,
		},
		"continue creating jobs after queue error": {
			monitor:         &MockQueueMonitor{3, 0, &MockJobRepository{failCount: 0}, 1},
			maxWorkers:      3,
			expectedWorkers: 3,
		},
	}
	t.Setenv("ANCHOR_BATCH_MONITOR_TICK", "100ms")
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("MAX_ANCHOR_WORKERS", strconv.FormatInt(int64(test.maxWorkers), 10))
			test.jobDb = test.monitor.jobDb
			t.Logf("start: unprocessed=%d, inflight=%d, max=%d", test.monitor.unprocessed, test.monitor.inFlight, test.maxWorkers)
			workerService := NewWorkerService(test.monitor, test.jobDb)
			// Cancel the context after at least 3 iterations of the ticker so that we can test how the service behaves
			// over time, especially when there are errors.
			ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
			workerService.Run(ctx)
			cancel()
			if len(test.jobDb.jobStore) != test.expectedWorkers {
				t.Errorf("incorrect number %d of workers created, expected %d", len(test.jobDb.jobStore), test.expectedWorkers)
			}
			numProcessed, numInFlight, _ := test.monitor.GetQueueUtilization(nil)
			t.Logf("end: unprocessed=%d, inflight=%d, created=%d", numProcessed, numInFlight, len(test.jobDb.jobStore))
		})
	}
}
