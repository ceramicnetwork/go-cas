package models

const AlertTitle = "CASv5 Alert"

const (
	AlertDesc_DeadLetterQueue = "Dead Letter Queue"
	AlertDesc_Unprocessed     = "Unprocessed Requests"
)

const (
	AlertFmt_DeadLetterQueue string = "%s:\n%s"
	AlertFmt_Unprocessed     string = "%d requests found since:\n%s"
)
