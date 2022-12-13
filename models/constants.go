package models

import "time"

// Defaults
const DefaultTick = 10 * time.Second
const DefaultHttpWaitTime = 10 * time.Second

// TaskQueue/BatchExecutor
const MaxNumTaskWorkers = 8
const MaxRequestBatchLinger = time.Minute
