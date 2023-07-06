package models

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type AnchorRepository interface {
	GetRequests(ctx context.Context, status RequestStatus, since time.Time, limit int) ([]*AnchorRequest, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status RequestStatus, allowedSourceStatuses []RequestStatus) error
}

type StateRepository interface {
	GetCheckpoint(ctx context.Context, checkpointType CheckpointType) (time.Time, error)
	UpdateCheckpoint(ctx context.Context, checkpointType CheckpointType, checkpoint time.Time) (bool, error)
	StoreCid(ctx context.Context, streamCid *StreamCid) (bool, error)
	UpdateTip(ctx context.Context, newTip *StreamTip) (bool, *StreamTip, error)
}

type JobRepository interface {
	CreateJob(ctx context.Context) (string, error)
	QueryJob(ctx context.Context, id string) (*JobState, error)
}

type QueuePublisher interface {
	SendMessage(ctx context.Context, event any) (string, error)
}

type QueueMonitor interface {
	GetQueueUtilization(ctx context.Context) (int, int, error)
}

type Notifier interface {
	SendAlert(title, desc string) error
}

type MetricService interface {
	Count(ctx context.Context, name MetricName, val int) error
	Shutdown(ctx context.Context)
}
