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
