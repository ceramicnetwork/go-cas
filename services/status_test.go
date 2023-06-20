package services

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/google/uuid"

	"github.com/ceramicnetwork/go-cas/models"
)

func TestStatus(t *testing.T) {
	replacedStatusRequest := models.RequestStatusMessage{Id: uuid.New(), Status: models.RequestStatus_Replaced}
	replacedStatus, _ := json.Marshal(replacedStatusRequest)
	tests := map[string]struct {
		anchorDb              *MockAnchorRepository
		statusRequestStr      string
		allowedSourceStatuses []models.RequestStatus
		shouldError           bool
	}{
		"update status in db": {
			anchorDb:              &MockAnchorRepository{},
			statusRequestStr:      string(replacedStatus),
			allowedSourceStatuses: []models.RequestStatus{models.RequestStatus_Pending},
			shouldError:           false,
		},
		"return error for invalid request without updating db": {
			anchorDb:         &MockAnchorRepository{},
			statusRequestStr: "invalid request",
			shouldError:      true,
		},
		"return error if db write failed": {
			anchorDb:         &MockAnchorRepository{shouldFailUpdate: true},
			statusRequestStr: string(replacedStatus),
			shouldError:      true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			statusService := NewStatusService(test.anchorDb)
			if err := statusService.Status(context.Background(), test.statusRequestStr); err != nil && !test.shouldError {
				t.Errorf("unexpected error received %v", err)
			} else if err == nil && test.shouldError {
				t.Errorf("should have received error")
			}
			if test.shouldError {
				if test.anchorDb.getNumUpdates() != 0 {
					t.Errorf("db should not have been updated")
				}
			} else if test.anchorDb.getNumUpdates() != 1 {
				t.Errorf("db should have been updated")
			} else {
				statusUpd := test.anchorDb.getStatusUpdate(replacedStatusRequest.Id)
				if statusUpd.newStatus != replacedStatusRequest.Status {
					t.Errorf("invalid status update: found=%v, expected=%v", statusUpd.newStatus, replacedStatusRequest.Status)
				}
				if !reflect.DeepEqual(statusUpd.allowedSourceStatuses, test.allowedSourceStatuses) {
					t.Errorf("disallowed source status found: found=%v, expected=%v", statusUpd.allowedSourceStatuses, test.allowedSourceStatuses)
				}
			}
		})
	}
}
