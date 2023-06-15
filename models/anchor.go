package models

type RequestStatus uint8

const (
	RequestStatus_Pending RequestStatus = iota
	RequestStatus_Processing
	RequestStatus_Completed
	RequestStatus_Failed
	RequestStatus_Ready
	RequestStatus_Replaced
)
