package common

import "time"

const DefaultRpcWaitTime = 30 * time.Second

const DbDateFormat = "2006-01-02 15:04:05.000000"

const ServiceName = "cas-scheduler"

const (
	Env_MetricsEndpoint = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
	Env_DbHost          = "DB_HOST"
	Env_DbName          = "DB_NAME"
	Env_DbPassword      = "DB_PASSWORD"
	Env_DbPort          = "DB_PORT"
	Env_DbUsername      = "DB_USERNAME"
)
