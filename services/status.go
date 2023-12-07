package services

import (
	"context"
	"encoding/json"

	"github.com/ceramicnetwork/go-cas/models"
)

type StatusService struct {
	anchorDb      models.AnchorRepository
	metricService models.MetricService
	logger        models.Logger
}

func NewStatusService(anchorDb models.AnchorRepository, metricService models.MetricService, logger models.Logger) *StatusService {
	return &StatusService{anchorDb, metricService, logger}
}

func (s StatusService) Status(ctx context.Context, msgBody string) error {
	statusMsg := new(models.RequestStatusMessage)
	if err := json.Unmarshal([]byte(msgBody), statusMsg); err != nil {
		return err
	} else {
		s.metricService.Count(ctx, models.MetricName_StatusIngressMessage, 1)
		s.logger.Debugw("status: dequeued",
			"msg", statusMsg,
		)
		var allowedSourceStatuses []models.RequestStatus
		switch statusMsg.Status {
		case models.RequestStatus_Replaced:
			// Can only transition to REPLACED from PENDING
			allowedSourceStatuses = []models.RequestStatus{models.RequestStatus_Pending}
		}
		if err = s.anchorDb.UpdateStatus(ctx, statusMsg.Id, statusMsg.Status, allowedSourceStatuses); err != nil {
			return err
		}
		s.metricService.Count(ctx, models.MetricName_StatusUpdated, 1)
		s.logger.Debugw("status: updated",
			"msg", statusMsg,
		)
	}
	return nil
}
