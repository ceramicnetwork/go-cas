package models

import (
	"time"

	"github.com/google/uuid"
)

type AnchorRequestMessage struct {
	Id         uuid.UUID   `json:"rid"`
	StreamId   string      `json:"sid"`
	Cid        string      `json:"cid"`
	UpdatedAt  time.Time   `json:"ts"`
	Attempt    *int        `json:"att,omitempty"`
	GenesisCid *string     `json:"gid,omitempty"`
	StreamType *StreamType `json:"st,omitempty"`
}

type RequestStatusMessage struct {
	Id      uuid.UUID `json:"rid"`
	Attempt int       `json:"att"`
	Loaded  bool      `json:"ld"`
}
