package models

import "time"

const DbDateFormat = "2006-01-02 15:04:05.000000"
const DbLoadLimit = 5000

// Defaults
const DefaultTick = 1 * time.Second
const DefaultHttpWaitTime = 10 * time.Second
const DefaultBatchMaxDepth = 10
const DefaultBatchMaxLinger = 10 * time.Second
const DefaultRateLimit = 10 // per second
const DefaultQueueDepthLimit = 25
const DefaultMaxReceivedMessages = 100
const DefaultMaxNumWorkers = 4
const DefaultMaxInflightMessages = 100

type QueueType string

const (
	QueueType_Request    QueueType = "request"
	QueueType_Multiquery QueueType = "multiquery"
	QueueType_Ready      QueueType = "ready"
	QueueType_Worker     QueueType = "worker"
	QueueType_Failure    QueueType = "failure"
)
