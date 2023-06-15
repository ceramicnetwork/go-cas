package models

import (
	"context"
	"time"
)

type AnchorRepository interface {
	GetRequests(RequestStatus, time.Time, int) ([]*AnchorRequestMessage, error)
	UpdateStatus(context.Context, *RequestStatusMessage) error
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
