package models

import "time"

const DbDateFormat = "2006-01-02 15:04:05.000000"
const DbLoadLimit = 100

// Defaults
const DefaultTick = 1 * time.Second
const DefaultHttpWaitTime = 10 * time.Second
const DefaultBatchMaxDepth = 10
const DefaultBatchMaxLinger = 1 * time.Second
const DefaultRateLimit = 10 // per second
const DefaultQueueDepthLimit = 25
const DefaultAnchorBatchSize = 1024
const DefaultAnchorBatchLinger = 12 * time.Hour

type QueueType string

const (
	QueueType_Ready QueueType = "ready"
	QueueType_Batch QueueType = "batch"
)
