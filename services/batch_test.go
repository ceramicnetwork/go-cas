package services

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/ceramicnetwork/go-cas/models"
	"github.com/google/uuid"
)

func TestBatch(t *testing.T) {
	t.Setenv("ANCHOR_BATCH_SIZE", "3")
	t.Setenv("ANCHOR_BATCH_LINGER", "5s")
	requests := []*models.AnchorRequestMessage{
		{Id: uuid.New()},
		{Id: uuid.New()},
		{Id: uuid.New()},
		{Id: uuid.New()},
		{Id: uuid.New()},
	}

	encodedRequests := make([]string, len(requests))
	for i, request := range requests {
		if requestMessage, err := json.Marshal(request); err != nil {
			t.Fatalf("Failed to encode request %v", request)
		} else {
			encodedRequests[i] = string(requestMessage)
		}
	}

	tests := map[string]struct {
		publisher                        *FakePublisher
		expectedNumberOfRequestsPerBatch []int
		shouldError                      bool
		encodedRequests                  []string
	}{
		"Can create batches after linger and when full": {
			publisher:                        &FakePublisher{messages: make(chan any, 2)},
			expectedNumberOfRequestsPerBatch: []int{3, 2},
			shouldError:                      false,
			encodedRequests:                  encodedRequests,
		},
		"Should return error if requests are malformed": {
			publisher:                        &FakePublisher{messages: make(chan any, 2)},
			expectedNumberOfRequestsPerBatch: []int{},
			shouldError:                      true,
			encodedRequests:                  []string{"hello"},
		},
		"Should return error if cannot publish batch": {
			publisher:                        &FakePublisher{messages: make(chan any, 2), errorOn: 1},
			expectedNumberOfRequestsPerBatch: []int{},
			shouldError:                      true,
			encodedRequests:                  encodedRequests[:3],
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			jobRepo := &FakeJobRepository{jobStore: make(map[string]bool)}
			batchingServices := NewBatchingService(test.publisher, jobRepo)
			ctx, cancel := context.WithCancel(context.Background())

			var wg sync.WaitGroup
			for _, er := range test.encodedRequests {
				// 5 request are batched simultaneously via go routines
				// 1 batch is created with 3 requests (the anchor batch size is 3)
				// after 5 seconds (the anchor batch linger is 5s) the 2nd batch is created containing 2 requests
				// if errors occur no batches are created and errors are returned
				encodedRequest := er
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := batchingServices.Batch(ctx, encodedRequest)

					if err != nil && !test.shouldError {
						t.Errorf("Unexpected error received %v", err)
					} else if err == nil && test.shouldError {
						t.Errorf("Should have received error")
					}
				}()
			}

			if test.shouldError {
				// if an error occurs no batch messages should have been sent
				wg.Wait()
				cancel()
				if len(test.publisher.messages) != 0 {
					t.Errorf("Received %v messages but should have received none", len(test.publisher.messages))
				}
				if len(jobRepo.jobStore) != 0 {
					t.Errorf("Found %v worker jobs but should have found none", len(jobRepo.jobStore))
				}
			} else {
				// with 5 requests 2 batches should have been created
				receivedMessages := waitForMesssages(test.publisher.messages, 2)
				wg.Wait()
				cancel()

				// decode the messages
				receivedBatches := make([]models.AnchorBatchMessage, len(receivedMessages))
				for i, message := range receivedMessages {
					if batch, ok := message.(models.AnchorBatchMessage); !ok {
						t.Fatalf("Received invalid anchor batch message: %v", message)
					} else {
						receivedBatches[i] = batch
					}
				}

				// make sure the batches have the correct number of requests in them, cannot ensure which request are in which batch
				// as the batch requests are made simultaneously
				for i, numRequestsInBatch := range test.expectedNumberOfRequestsPerBatch {
					if len(receivedBatches[i].Ids) != numRequestsInBatch {
						t.Errorf("Expected %v requests in batch %v. Contained %v requests", numRequestsInBatch, i+1, len(receivedBatches[i].Ids))
					}
				}
				// make sure that a worker job is create for each batch successfully created
				if len(jobRepo.jobStore) != len(test.expectedNumberOfRequestsPerBatch) {
					t.Errorf("Expected %v worker jobs. Found %v jobs", len(test.expectedNumberOfRequestsPerBatch), len(jobRepo.jobStore))
				}
			}
		})
	}
}
