package services

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/smrz2001/go-cas/models"
)

type anchorRepository interface {
	GetRequests(models.RequestStatus, time.Time, time.Time, []string, int) ([]*models.AnchorRequestMessage, error)
	UpdateStatus(uuid.UUID, models.RequestStatus, string) error
}

type stateRepository interface {
	GetCheckpoint(models.CheckpointType) (time.Time, error)
	UpdateCheckpoint(models.CheckpointType, time.Time) (bool, error)
	UpdateCid(*models.StreamCid) error
	GetStreamTip(string) (*models.StreamCid, error)
}

type queuePublisher interface {
	SendMessage(ctx context.Context, event any) (string, error)
}

type ceramicClient interface {
	Pin(context.Context, string) (*models.CeramicPinResult, error)
	Query(context.Context, string) (*models.StreamState, error)
	Multiquery(context.Context, []*models.CeramicQuery) (map[string]*models.StreamState, error)
	MultiqueryId(*models.CeramicQuery) string
}
