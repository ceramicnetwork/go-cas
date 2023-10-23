package models

import (
	"time"

	"github.com/google/uuid"
)

const DeployComponent = "casv5"
const WorkerVersion = "5"
const DefaultJobTtl = 2 * 7 * 24 * time.Hour

type JobType string

const (
	JobType_Deploy JobType = "deploy"
	JobType_Anchor JobType = "anchor"
)

type JobStage string

const (
	JobStage_Queued    JobStage = "queued"
	JobStage_Waiting   JobStage = "waiting"
	JobStage_Failed    JobStage = "failed"
	JobStage_Completed JobStage = "completed"
)

const (
	JobParam_Overrides = "overrides"
	JobParam_Version   = "version"
)

const (
	AnchorOverrides_AppMode                = "APP_MODE"
	AnchorOverrides_ContractAddress        = "ETH_CONTRACT_ADDRESS"
	AnchorOverrides_SchedulerStopAfterNoOp = "SCHEDULER_STOP_AFTER_NO_OP"
)

const (
	AnchorAppMode_Anchor             = "anchor"
	AnchorAppMode_ContinualAnchoring = "continual-anchoring"
)

type JobState struct {
	Job    string                 `dynamodbav:"job"`
	Stage  JobStage               `dynamodbav:"stage"`
	Type   JobType                `dynamodbav:"type"`
	Ts     time.Time              `dynamodbav:"ts"`
	Params map[string]interface{} `dynamodbav:"params"`
	Id     string                 `dynamodbav:"id" json:"-"`
	Ttl    time.Time              `dynamodbav:"ttl,unixtime" json:"-"`
}

func NewJob(jobType JobType, params map[string]interface{}) JobState {
	return JobState{
		Job:    uuid.New().String(),
		Stage:  JobStage_Queued,
		Type:   jobType,
		Ts:     time.Now(),
		Params: params,
		Id:     uuid.New().String(),
		Ttl:    time.Now().Add(DefaultJobTtl),
	}
}
