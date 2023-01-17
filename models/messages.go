package models

import (
	"time"

	"github.com/google/uuid"
)

type AnchorRequestMessage struct {
	Id         uuid.UUID   `json:"rid"`
	StreamId   string      `json:"sid"`
	Cid        string      `json:"cid"`
	CreatedAt  time.Time   `json:"ts"`
	GenesisCid *string     `json:"gid,omitempty"`
	StreamType *StreamType `json:"st,omitempty"`
}

type StreamReadyMessage struct {
	StreamId string `json:"sid"`
}

type AnchorWorkerMessage struct {
	StreamIds []string `json:"sids"`
}
