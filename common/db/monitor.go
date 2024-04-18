package db

import (
	"context"

	"github.com/ceramicnetwork/go-cas/models"
)

type monitor struct {
	anchorDb models.AnchorRepository
}

func NewDbMonitor(anchorDb models.AnchorRepository) models.ResourceMonitor {
	return &monitor{anchorDb}
}

func (m monitor) GetValue(ctx context.Context) (int, error) {
	return m.anchorDb.RequestCount(ctx, models.RequestStatus_Pending)
}
