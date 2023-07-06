package models

import (
	"time"

	"github.com/google/uuid"
)

const DeployComponent = "casv5"
const WorkerVersion = "5"

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

const JobParam_Version = "version"

type JobState struct {
	Stage  JobStage               `dynamodbav:"stage"`
	Ts     time.Time              `dynamodbav:"ts"`
	Id     string                 `dynamodbav:"id"`
	Type   JobType                `dynamodbav:"type"`
	Params map[string]interface{} `dynamodbav:"params"`
}

func NewJob(jobType JobType, params map[string]interface{}) JobState {
	return JobState{
		Stage:  JobStage_Queued,
		Ts:     time.Time{},
		Id:     uuid.New().String(),
		Type:   jobType,
		Params: params,
	}
}
