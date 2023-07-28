package services

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ceramicnetwork/go-cas/models"
)

type FailureHandlingService struct {
	notif         models.Notifier
	metricService models.MetricService
}

func NewFailureHandlingService(notif models.Notifier, metricService models.MetricService) *FailureHandlingService {
	return &FailureHandlingService{notif, metricService}
}

func (f FailureHandlingService) Failure(ctx context.Context, _ string) error {
	// TODO: Implement handling for failures reported by other services
	f.metricService.Count(ctx, models.MetricName_FailureMessage, 1)
	return nil
}

func (f FailureHandlingService) DLQ(ctx context.Context, msgBody string) error {
	f.metricService.Count(ctx, models.MetricName_FailureDlqMessage, 1)
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
	return f.notif.SendAlert(models.ErrorTitle, fmt.Sprintf(models.ErrorMessageFmt_DLQ, msgType, msgBody))
}
