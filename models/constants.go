package models

import "time"

const DbDateFormat = "2006-01-02 15:04:05.000000"
const DbLoadLimit = 250

const MaxBatchProcessingTime = 5 * time.Minute

// Defaults
const DefaultTick = 10 * time.Second
const DefaultHttpWaitTime = 10 * time.Second

// TaskQueue/BatchExecutor
const TaskQueueMaxWorkers = 32
const TaskQueueMaxQueueDepth = 100
const BatchMaxDepth = 25
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
