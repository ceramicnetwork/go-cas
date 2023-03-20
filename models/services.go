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
	StoreCid(*StreamCid) (bool, error)
	GetCid(string, string) (*StreamCid, error)
	GetTipCid(string) (*StreamCid, error)
	UpdateAnchorTs(string, string, time.Time) (bool, error)
	GetAnchoredCid(string, string) (*StreamCid, error)
}

type QueuePublisher interface {
	SendMessage(ctx context.Context, event any) (string, error)
}
