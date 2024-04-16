package models

type MetricName string

// Counts
const (
	MetricName_BatchCreated               MetricName = "batch_created"
	MetricName_BatchIngressRequest        MetricName = "batch_ingress_request"
	MetricName_BatchSize                  MetricName = "batch_size"
	MetricName_BatchStored                MetricName = "batch_stored"
	MetricName_FailureDlqMessage          MetricName = "failure_dlq_message"
	MetricName_FailureMessage             MetricName = "failure_message"
	MetricName_StatusIngressMessage       MetricName = "status_ingress_message"
	MetricName_StatusUpdated              MetricName = "status_updated"
	MetricName_ValidateIngressRequest     MetricName = "validate_ingress_request"
	MetricName_ValidateReplacedRequest    MetricName = "validate_replaced_request"
	MetricName_ValidateProcessedRequest   MetricName = "validate_processed_request"
	MetricName_ValidateReprocessedRequest MetricName = "validate_reprocessed_request"
	MetricName_WorkerJobCreated           MetricName = "worker_job_created"
	MetricName_IpfsError                  MetricName = "ipfs_error"
	MetricName_IpfsUnknownTask            MetricName = "ipfs_unknown_task"
	MetricName_IpfsPubsubPublishExpired   MetricName = "ipfs_pubsub_publish_expired"
)

const MetricsCallerName = "go-cas"
