package services

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	tests := map[string]struct {
		monitor      *MockQueueMonitor
		jobDb        *MockJobRepository
		maxWorkers   int
		inflightJobs int
		finishedJobs int
		newJobs      int
	}{
		"create as many jobs as unprocessed batches": {
			monitor:    &MockQueueMonitor{5, 0},
			jobDb:      &MockJobRepository{failCount: 0},
			maxWorkers: -1,
			newJobs:    5,
		},
		"create jobs upto configured max": {
			monitor:    &MockQueueMonitor{3, 0},
			jobDb:      &MockJobRepository{failCount: 0},
			maxWorkers: 2,
			newJobs:    2,
		},
		"do not create jobs if max already in flight": {
			monitor:      &MockQueueMonitor{3, 0},
			jobDb:        &MockJobRepository{failCount: 0},
			maxWorkers:   2,
			inflightJobs: 2,
			newJobs:      0,
		},
		"do not create jobs if no unprocessed batches": {
			monitor:    &MockQueueMonitor{0, 0},
			jobDb:      &MockJobRepository{failCount: 0},
			maxWorkers: -1,
			newJobs:    0,
		},
		"create only as many jobs as necessary": {
			monitor:    &MockQueueMonitor{1, 0},
			jobDb:      &MockJobRepository{failCount: 0},
			maxWorkers: 2,
			newJobs:    1,
		},
		"continue creating jobs after db error": {
			monitor:    &MockQueueMonitor{3, 0},
			jobDb:      &MockJobRepository{failCount: 1},
			maxWorkers: 3,
			newJobs:    3,
		},
		"continue creating jobs after queue error": {
			monitor:    &MockQueueMonitor{3, 1},
			jobDb:      &MockJobRepository{failCount: 0},
			maxWorkers: 3,
			newJobs:    3,
		},
		"resume creating jobs if existing job finishes": {
			monitor:      &MockQueueMonitor{3, 1},
			jobDb:        &MockJobRepository{failCount: 0},
			maxWorkers:   2,
			inflightJobs: 2,
			finishedJobs: 1,
			newJobs:      1,
		},
	}
	t.Setenv("ANCHOR_BATCH_MONITOR_TICK", "100ms")
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("MAX_ANCHOR_WORKERS", strconv.FormatInt(int64(test.maxWorkers), 10))
			workerService := NewWorkerService(test.monitor, test.jobDb)
			if test.inflightJobs > 0 {
				// Pre-run the service so that it creates jobs in flight
				ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
				workerService.Run(ctx)
				cancel()

				if len(test.jobDb.jobStore) != test.inflightJobs {
					t.Errorf("incorrect number %d of jobs, expected %d", len(test.jobDb.jobStore), test.inflightJobs)
				}
			}
			if test.finishedJobs > 0 {
				// Complete some jobs
				test.jobDb.finishJobs(test.finishedJobs)
			}
			// Cancel the context after at least 3 iterations of the ticker so that we can test how the service behaves
			// over time, especially when there are errors.
			ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
			workerService.Run(ctx)
			cancel()
			numJobsRemaining := test.inflightJobs + test.newJobs - test.finishedJobs
			if len(test.jobDb.jobStore) != numJobsRemaining {
				t.Errorf("incorrect number %d of remaining jobs, expected %d", len(test.jobDb.jobStore), numJobsRemaining)
			}
		})
	}
}
