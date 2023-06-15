package services

import (
	"context"
	"encoding/json"

	"github.com/ceramicnetwork/go-cas/models"
)

type StatusService struct {
	anchorDb models.AnchorRepository
}

func NewStatusService(anchorDb models.AnchorRepository) *StatusService {
	return &StatusService{anchorDb}
}

func (s StatusService) Status(ctx context.Context, msgBody string) error {
	statusMsg := new(models.RequestStatusMessage)
	if err := json.Unmarshal([]byte(msgBody), statusMsg); err != nil {
		return err
	} else if err = s.anchorDb.UpdateStatus(ctx, statusMsg); err != nil {
		return err
	}
	return nil
}
