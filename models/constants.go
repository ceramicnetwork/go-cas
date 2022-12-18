package models

import "time"

const DbDateFormat = "2006-01-02 15:04:05.000000"
const DbLoadLimit = 1000

const MaxBatchProcessingTime = 5 * time.Minute

// Defaults
const DefaultTick = 1 * time.Second
const DefaultHttpWaitTime = 10 * time.Second

// TaskQueue/BatchExecutor
const TaskQueueMaxWorkers = 8
const TaskQueueMaxQueueDepth = 8
const BatchMaxDepth = 10
const BatchMaxLinger = 10 * time.Second

type CommitType uint8

const (
	CommitType_Genesis CommitType = iota
	CommitType_Signed
	CommitType_Anchor
)

type StreamType uint8

const (
	StreamType_Tile StreamType = iota
)
