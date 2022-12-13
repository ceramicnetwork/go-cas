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
)

type AnchorRequest struct {
	Id        uuid.UUID `json:"reqId"`
	StreamId  string    `json:"streamId"`
	Cid       string    `json:"cid"`
	CreatedAt time.Time `json:"ts"`
	// Not (de)serialized
	Status    int       `json:"-"`
	Message   string    `json:"-"`
	UpdatedAt time.Time `json:"-"`
	Pinned    bool      `json:"-"`
}
