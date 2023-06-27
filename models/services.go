package models

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type AnchorRepository interface {
	GetRequests(context.Context, RequestStatus, time.Time, int) ([]*AnchorRequest, error)
	UpdateStatus(context.Context, uuid.UUID, RequestStatus, []RequestStatus) error
}

type StateRepository interface {
	GetCheckpoint(CheckpointType) (time.Time, error)
	UpdateCheckpoint(CheckpointType, time.Time) (bool, error)
	StoreCid(*StreamCid) (bool, error)
	UpdateTip(*StreamTip) (bool, *StreamTip, error)
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

type MetricService interface {
	Count(ctx context.Context, name string, val int) error
	Shutdown(ctx context.Context)
}
