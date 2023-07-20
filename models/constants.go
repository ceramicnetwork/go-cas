package models

import "time"

const DefaultAnchorBatchSize = 1024
const DefaultAnchorBatchLinger = 12 * time.Hour

const (
	Env_AnchorBatchSize       = "ANCHOR_BATCH_SIZE"
	Env_AnchorContractAddress = "ANCHOR_CONTRACT_ADDRESS"
)
