package services

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/ceramicnetwork/go-cas/models"
)

type FailureHandlingService struct {
	notif         models.Notifier
	metricService models.MetricService
	logger        models.Logger
}

func NewFailureHandlingService(notif models.Notifier, metricService models.MetricService, logger models.Logger) *FailureHandlingService {
	return &FailureHandlingService{notif, metricService, logger}
}

func (f FailureHandlingService) Failure(ctx context.Context, _ string) error {
	// TODO: Implement handling for failures reported by other services
	f.metricService.Count(ctx, models.MetricName_FailureMessage, 1)
	return nil
}

func (f FailureHandlingService) DLQ(ctx context.Context, msgBody string) error {
	f.metricService.Count(ctx, models.MetricName_FailureDlqMessage, 1)
	msgType := "Unknown"
	msgId := "Unknown"
	// Unmarshal into one of the known message types
	anchorReq := new(models.AnchorRequestMessage)
	if err := json.Unmarshal([]byte(msgBody), anchorReq); (err == nil) && (anchorReq.Id != uuid.Nil) {
		msgType = "Anchor request"
		msgId = anchorReq.Id.String()
		f.logger.Debugw("dlq: dequeued",
			msgType, anchorReq,
		)
	} else {
		batchReq := new(models.AnchorBatchMessage)
		if err = json.Unmarshal([]byte(msgBody), batchReq); (err == nil) && (batchReq.Id != uuid.Nil) {
			msgType = "Batch request"
			msgId = batchReq.Id.String()
			f.logger.Debugw("dlq: dequeued",
				msgType, batchReq,
			)
		}
	}
	// Only include the message type and ID in the alert message since the message body is already logged, and Discord
	// messages have a character limit.
	return f.notif.SendAlert(
		models.AlertTitle,
		models.AlertDesc_DeadLetterQueue,
		fmt.Sprintf(models.AlertFmt_DeadLetterQueue, msgType, msgId),
	)
}
