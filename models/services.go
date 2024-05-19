package models

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/3box/pipeline-tools/cd/manager/common/job"
)

type AnchorRepository interface {
	GetRequests(ctx context.Context, status RequestStatus, newerThan time.Time, olderThan time.Time, limit int) ([]*AnchorRequest, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status RequestStatus, allowedSourceStatuses []RequestStatus) error
	RequestCount(ctx context.Context, status RequestStatus) (int, error)
}

type StateRepository interface {
	GetCheckpoint(ctx context.Context, checkpointType CheckpointType) (time.Time, error)
	UpdateCheckpoint(ctx context.Context, checkpointType CheckpointType, checkpoint time.Time) (bool, error)
	StoreCid(ctx context.Context, streamCid *StreamCid) (bool, error)
	UpdateTip(ctx context.Context, newTip *StreamTip) (bool, *StreamTip, error)
}

type JobRepository interface {
	CreateJob(ctx context.Context) (string, error)
	QueryJob(ctx context.Context, id string) (*job.JobState, error)
}

type KeyValueRepository interface {
	Store(ctx context.Context, key string, value interface{}) error
}

type QueuePublisher interface {
	GetUrl() string
	SendMessage(ctx context.Context, event any) (string, error)
}

type QueueMonitor interface {
	GetUtilization(ctx context.Context) (int, int, error)
}

type ResourceMonitor interface {
	GetValue(ctx context.Context) (int, error)
}

type Notifier interface {
	SendAlert(title, desc, content string) error
}

type MetricService interface {
	Count(ctx context.Context, name MetricName, val int) error
	Gauge(ctx context.Context, name MetricName, monitor ResourceMonitor) error
	Distribution(ctx context.Context, name MetricName, val int) error
	QueueGauge(ctx context.Context, queueName string, monitor QueueMonitor) error
	Shutdown(ctx context.Context)
}

type IpfsApi interface {
	Publish(ctx context.Context, topic string, data []byte) error
}

type Logger interface {
	Debugf(template string, args ...interface{})
	Debugw(msg string, args ...interface{})
	Errorf(template string, args ...interface{})
	Fatalf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Infoln(args ...interface{})
	Sync() error
}
