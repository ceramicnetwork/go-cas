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
	JobParam_Version   = "version"
	JobParam_Overrides = "overrides"
)

// These need to match the Anchor Worker environment variable names
const (
	AnchorOverrides_UseQueueBatches = "USE_QUEUE_BATCHES"
	AnchorOverrides_ContractAddress = "ETH_CONTRACT_ADDRESS"
	AnchorOverrides_BatchQueueUrl   = "SQS_BATCH_QUEUE_URL"
	AnchorOverrides_FailureQueueUrl = "SQS_FAILURE_QUEUE_URL"
)
