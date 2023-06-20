package models

import (
	"time"

	"github.com/google/uuid"
)

type RequestStatus uint8

const (
	RequestStatus_Pending RequestStatus = iota
	RequestStatus_Processing
	RequestStatus_Completed
	RequestStatus_Failed
	RequestStatus_Ready
	RequestStatus_Replaced
)

type AnchorRequest struct {
	Id        uuid.UUID
	Cid       string
	StreamId  string
	Origin    string
	Timestamp time.Time
	CreatedAt time.Time
	Metadata  *StreamMetadata
}
