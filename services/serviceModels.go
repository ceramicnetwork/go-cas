package services

import (
	"context"
	"time"

	"github.com/smrz2001/go-cas/models"
)

type anchorRepository interface {
	RequestsSinceCheckpoint(checkpoint time.Time, limit int) ([]*models.AnchorRequestMessage, error)
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
	Query(context.Context, string) (*models.StreamState, error)
	Multiquery(context.Context, []*models.CeramicQuery) (map[string]*models.StreamState, error)
	MultiqueryId(*models.CeramicQuery) string
}
