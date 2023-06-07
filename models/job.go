package models

const DefaultJobState = "queued"
const DeployComponent = "casv5"
const WorkerVersion = "5"

const (
	JobType_Deploy = "deploy"
	JobType_Anchor = "anchor"
)

const (
	JobParam_Id        = "id"
	JobParam_Ts        = "ts"
	JobParam_Stage     = "stage"
	JobParam_Type      = "type"
	JobParam_Params    = "params"
	JobParams_Version  = "version"
	JobParams_Contract = "contract"
)
