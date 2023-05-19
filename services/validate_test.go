package services

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/ceramicnetwork/go-cas/models"
	"github.com/google/uuid"
)

func TestValidate(t *testing.T) {
	existingStreamCid := &models.StreamCid{
		StreamId:  "existing",
		Cid:       "existing",
		Timestamp: time.Now().Round(0),
	}
	stateRepo := &FakeStateRepository{cidStore: make(map[string]bool)}
	stateRepo.StoreCid(existingStreamCid)

	tests := map[string]struct {
		publisher     *FakePublisher
		shouldError   bool
		shouldPublish bool
		request       *models.AnchorRequestMessage
	}{
		"Will not publish request if it already exists": {
			publisher:     &FakePublisher{messages: make(chan any, 1)},
			shouldError:   false,
			shouldPublish: false,
			request:       &models.AnchorRequestMessage{StreamId: existingStreamCid.StreamId, Cid: existingStreamCid.Cid, Timestamp: existingStreamCid.Timestamp, Id: uuid.New()},
		},
		"Will publish request if it does not exist": {
			publisher:     &FakePublisher{messages: make(chan any, 1)},
			shouldError:   false,
			shouldPublish: true,
			request:       &models.AnchorRequestMessage{StreamId: "unique", Cid: "unique", Timestamp: time.Now().Round(0), Id: uuid.New()},
		},
		"Will return error if we cannot publish to queue": {
			publisher:     &FakePublisher{messages: make(chan any, 1), errorOn: 1},
			shouldError:   true,
			shouldPublish: false,
			request:       &models.AnchorRequestMessage{StreamId: "unique2", Cid: "unique2", Timestamp: time.Now().Round(0), Id: uuid.New()},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			encodedRequest, err := json.Marshal(test.request)
			if err != nil {
				t.Fatalf("Error encoding the requests: %v", err)
			}

			validator := NewValidationService(stateRepo, test.publisher)
			err = validator.Validate(context.Background(), string(encodedRequest))
			// will error if we cannot publish to the queue
			if err != nil && !test.shouldError {
				t.Errorf("Unexpected error received %v", err)
			} else if err == nil && test.shouldError {
				t.Errorf("Should have received error")
			}

			// Will publish if the request is unique
			if test.shouldPublish {
				receivedMessage := waitForMesssages(test.publisher.messages, 1)[0]
				if receivedValidatedRequest, ok := receivedMessage.(*models.AnchorRequestMessage); !ok {
					t.Fatalf("Received invalid anchor request message: %v", receivedMessage)
				} else {
					if !reflect.DeepEqual(receivedValidatedRequest, test.request) {
						t.Errorf("incorrect request published: expected %v, got %v", test.request, receivedValidatedRequest)
					}
				}
			} else {
				// Will not publish if we have seen the request before
				if len(test.publisher.messages) != 0 {
					t.Errorf("Anchor request should not have been validated and published")
				}
			}

		})
	}

}
