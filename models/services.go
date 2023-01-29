package models

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type AnchorRepository interface {
	GetRequests(RequestStatus, time.Time, time.Time, []string, int) ([]*AnchorRequestMessage, error)
	UpdateStatus(uuid.UUID, RequestStatus, string) error
}

type StateRepository interface {
	GetCheckpoint(CheckpointType) (time.Time, error)
	UpdateCheckpoint(CheckpointType, time.Time) (bool, error)
	UpdateCid(*StreamCid) error
	GetStreamTip(string) (*StreamCid, error)
}

type QueuePublisher interface {
	SendMessage(ctx context.Context, event any) (string, error)
}

type CeramicClient interface {
	Pin(context.Context, string) (*CeramicPinResult, error)
	Query(context.Context, string) (*StreamState, error)
	Multiquery(context.Context, []*CeramicQuery) (map[string]*StreamState, error)
	MultiqueryId(*CeramicQuery) string
}
