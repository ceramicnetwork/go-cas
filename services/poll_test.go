package services

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"
)

var originalCheckpoint = time.Now().Add(-time.Hour)

func TestPoller(t *testing.T) {
	tests := map[string]struct {
		readyPublisher            *FakePublisher
		expectedCheckpoints       []time.Time
		expectedCurrentCheckpoint time.Time
	}{
		"Can poll": {
			readyPublisher:            &FakePublisher{messages: make(chan any, 4)},
			expectedCheckpoints:       []time.Time{originalCheckpoint, originalCheckpoint.Add(time.Minute * 2)},
			expectedCurrentCheckpoint: originalCheckpoint.Add(time.Minute * time.Duration(4)),
		},
		"Can poll when publishing initially failed": {
			readyPublisher:            &FakePublisher{messages: make(chan any, 4), errorOn: 1},
			expectedCheckpoints:       []time.Time{originalCheckpoint, originalCheckpoint.Add(time.Minute).Add(-time.Millisecond), originalCheckpoint.Add(-time.Millisecond).Add(time.Minute * 3)},
			expectedCurrentCheckpoint: originalCheckpoint.Add(-time.Millisecond).Add(time.Minute * 5),
		},
		"Can poll when publishing failed on middle request": {
			readyPublisher:            &FakePublisher{messages: make(chan any, 4), errorOn: 2},
			expectedCheckpoints:       []time.Time{originalCheckpoint, originalCheckpoint.Add(time.Minute), originalCheckpoint.Add(time.Minute * 3)},
			expectedCurrentCheckpoint: originalCheckpoint.Add(time.Minute * 5),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			anchorRepo := &SpyAnchorRepository{}
			stateRepo := &FakeStateRepository{checkpoint: originalCheckpoint}

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
