package services

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ceramicnetwork/go-cas/models"
)

type StatusService struct {
	anchorDb models.AnchorRepository
}

func NewStatusService(anchorDb models.AnchorRepository) *StatusService {
	return &StatusService{anchorDb}
}

// Status is the Loading service's message handler. It will be invoked for messages received on the Pin queue. The queue
// plumbing takes care of scaling the consumers, batching, etc.
//
// We won't return errors from here, which will cause the status update to be deleted from the originating queue.
// Requests that are not updated from here will get picked up in a subsequent iteration and reprocessed, which is ok.
func (s StatusService) Status(ctx context.Context, msgBody string) error {
	statusMsg := new(models.RequestStatusMessage)
	if err := json.Unmarshal([]byte(msgBody), statusMsg); err != nil {
		return err
	} else {
		status := models.RequestStatus_Failed
		if statusMsg.Loaded {
			// Put requests for loaded CIDs back into the PENDING state
			status = models.RequestStatus_Pending
		}
		var message string
		if statusMsg.Loaded {
			message = "Request is pending."
		} else {
			message = fmt.Sprintf("Reload failed #%d times.", statusMsg.Attempt+1)
		}
		if err = s.anchorDb.UpdateStatus(statusMsg.Id, status, message); err != nil {
			return err
		}
	}
	return nil
}
