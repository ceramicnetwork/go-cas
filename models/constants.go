package models

import "time"

const DbDateFormat = "2006-01-02 15:04:05.000000"
const DbLoadLimit = 100

// Defaults
const DefaultTick = 1 * time.Second
const DefaultHttpWaitTime = 10 * time.Second
const DefaultDbWaitTime = 30 * time.Second
const DefaultAnchorBatchSize = 1024
const DefaultAnchorBatchLinger = 12 * time.Hour
const DefaultAnchorBatchMonitorTick = 5 * time.Minute
const DefaultMaxAnchorWorkers = 1

const ServiceName = "cas-scheduler"

// Environment variables
const (
	Env_EnvTag = "ENV_TAG"
)
