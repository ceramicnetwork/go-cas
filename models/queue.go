package models

import "time"

type QueueType string

const (
	QueueType_Validate QueueType = "validate"
	QueueType_Ready    QueueType = "ready"
	QueueType_Batch    QueueType = "batch"
	QueueType_Failure  QueueType = "failure"
	QueueType_DLQ      QueueType = "dlq"
)

const QueueMaxLinger = 250 * time.Millisecond
const QueueDefaultVisibilityTimeout = 5 * time.Minute
const QueueMaxReceiveCount = 3

type QueueRedrivePolicy struct {
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	MaxReceiveCount     int    `json:"maxReceiveCount"`
}
