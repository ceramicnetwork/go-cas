package models

import "time"

const DefaultAnchorBatchSize = 1048576 // 2^20
const DefaultAnchorBatchLinger = time.Hour

const (
	Env_AnchorAuditEnabled    = "ANCHOR_AUDIT_ENABLED"
	Env_AnchorBatchSize       = "ANCHOR_BATCH_SIZE"
	Env_AnchorContractAddress = "ANCHOR_CONTRACT_ADDRESS"
)
