package services

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ceramicnetwork/go-cas/models"
)

const ErrorTitle = "CAS Scheduler Error"

const (
	ErrorMessageFmt_DLQ string = "%s message found in dead-letter queue: [%s]"
)

type FailureHandlingService struct {
	notif models.Notifier
}

func NewFailureHandlingService(notif models.Notifier) *FailureHandlingService {
	return &FailureHandlingService{notif}
}

func (b FailureHandlingService) Failure(_ context.Context, _ string) error {
	// TODO: Implement handling for failures reported by other services
	return nil
}

func (b FailureHandlingService) DLQ(ctx context.Context, msgBody string) error {
	msgType := "Unknown"
	// Unmarshal into one of the known message types
	anchorReq := new(models.AnchorRequestMessage)
	if err := json.Unmarshal([]byte(msgBody), anchorReq); err == nil {
		msgType = "Anchor request"
	} else {
		batchReq := new(models.AnchorBatchMessage)
		if err = json.Unmarshal([]byte(msgBody), batchReq); err == nil {
			msgType = "Batch request"
		}
	}
	return b.notif.SendAlert(ErrorTitle, fmt.Sprintf(ErrorMessageFmt_DLQ, msgType, msgBody))
}
