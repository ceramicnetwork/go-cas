package models

import (
	"time"

	"github.com/google/uuid"
)

type AnchorRequestMessage struct {
	Id        uuid.UUID       `json:"rid"`
	StreamId  string          `json:"sid"`
	Cid       string          `json:"cid"`
	Timestamp time.Time       `json:"ts"`
	Metadata  *StreamMetadata `json:"mta,omitempty"`
	CreatedAt time.Time
}

type AnchorBatchMessage struct {
	Id  uuid.UUID   `json:"bid"`
	Ids []uuid.UUID `json:"rids"`
}
