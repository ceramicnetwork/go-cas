package models

import (
	"time"

	"github.com/google/uuid"
)

type AnchorRequestEvent struct {
	Id         uuid.UUID   `json:"rid"`
	StreamId   string      `json:"sid"`
	Cid        string      `json:"cid"`
	CreatedAt  time.Time   `json:"ts"`
	GenesisCid *string     `json:"gid"`
	StreamType *StreamType `json:"st"`
}

type StreamReadyEvent struct {
	StreamId string `json:"sid"`
}

type AnchorWorkerEvent struct {
	StreamIds []string `json:"sids"`
}
