package models

const ErrorTitle = "CAS Scheduler Error"

const (
	ErrorMessageFmt_DLQ         string = "%s message found in dead-letter queue: [%s]"
	ErrorMessageFmt_Unprocessed string = "Unprocessed anchor requests found between %s and %s"
)
