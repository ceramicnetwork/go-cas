package models

import (
	"time"

	"github.com/google/uuid"

	"github.com/3box/pipeline-tools/cd/manager"
)

const DeployComponent = "casv5"
const WorkerVersion = "5"
const DefaultJobTtl = 2 * 7 * 24 * time.Hour

const (
	AnchorOverrides_AppMode                = "APP_MODE"
	AnchorOverrides_ContractAddress        = "ETH_CONTRACT_ADDRESS"
	AnchorOverrides_SchedulerStopAfterNoOp = "SCHEDULER_STOP_AFTER_NO_OP"
)

const (
	AnchorAppMode_Anchor             = "anchor"
	AnchorAppMode_ContinualAnchoring = "continual-anchoring"
)

func NewJob(jobType manager.JobType, params map[string]interface{}) manager.JobState {
	return manager.JobState{
		Job:    uuid.New().String(),
		Stage:  manager.JobStage_Queued,
		Type:   jobType,
		Ts:     time.Now(),
		Params: params,
		Id:     uuid.New().String(),
		Ttl:    time.Now().Add(DefaultJobTtl),
	}
}
