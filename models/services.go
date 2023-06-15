package models

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type AnchorRepository interface {
	GetRequests(RequestStatus, time.Time, int) ([]*AnchorRequestMessage, error)
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

type JobRepository interface {
	CreateJob() error
}

type QueuePublisher interface {
	SendMessage(ctx context.Context, event any) (string, error)
}

type QueueMonitor interface {
	GetQueueUtilization(ctx context.Context) (int, int, error)
}

type Notifier interface {
	SendAlert(string, string) error
}
