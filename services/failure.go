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
	// Unmarshal into one of the known message types
	anchorReq := new(models.AnchorRequestMessage)
	if err := json.Unmarshal([]byte(msgBody), anchorReq); err == nil {
		msgType = "Anchor request"
		f.logger.Debugw("dlq: dequeued",
			msgType, anchorReq,
		)
	} else {
		batchReq := new(models.AnchorBatchMessage)
		if err = json.Unmarshal([]byte(msgBody), batchReq); err == nil {
			msgType = "Batch request"
			f.logger.Debugw("dlq: dequeued",
				msgType, batchReq,
			)
		}
	}
	text, _ := json.MarshalIndent(msgBody, "", "")
	return f.notif.SendAlert(
		models.AlertTitle,
		models.AlertDesc_DeadLetterQueue,
		fmt.Sprintf(models.AlertFmt_DeadLetterQueue, msgType, text),
	)
}
