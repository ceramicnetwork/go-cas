package common

import "time"

const DefaultRpcWaitTime = 10 * time.Second

const DbDateFormat = "2006-01-02 15:04:05.000000"

const ServiceName = "cas-scheduler"

const (
	Env_CollectorHost = "COLLECTOR_HOST"
	Env_PgHost        = "PG_HOST"
	Env_PgDb          = "PG_DB"
	Env_PgPassword    = "PG_PASSWORD"
	Env_PgPort        = "PG_PORT"
	Env_PgUser        = "PG_USER"
)
