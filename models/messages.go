package models

import (
	"time"

	"github.com/google/uuid"
)

type AnchorRequestMessage struct {
	Id        uuid.UUID `json:"rid"`
	StreamId  string    `json:"sid"`
	Cid       string    `json:"cid"`
	Origin    string    `json:"org"`
	Timestamp time.Time `json:"ts"`
	CreatedAt time.Time `json:"crt"`
}

type AnchorBatchMessage struct {
	Id  uuid.UUID   `json:"bid"`
	Ids []uuid.UUID `json:"rids,omitempty"`
}

type RequestStatusMessage struct {
	Id     uuid.UUID     `json:"rid"`
	Status RequestStatus `json:"sts"`
}

type IpfsPubsubPublishMessage struct {
	CreatedAt time.Time `json:"createdAt" validate:"required"`
	Topic     string    `json:"topic" validate:"required"`
	Data      []byte    `json:"data" validate:"required"`
}
