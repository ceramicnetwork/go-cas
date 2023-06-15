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
	Ids []uuid.UUID `json:"rids"`
}

type RequestStatusMessage struct {
	Id     uuid.UUID     `json:"rid"`
	Status RequestStatus `json:"sts"`
}
