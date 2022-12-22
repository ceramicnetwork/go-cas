package models

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type AnchorRequest struct {
	Id        uuid.UUID `json:"id"`
	StreamId  string    `json:"streamId"`
	Cid       string    `json:"cid"`
	CreatedAt time.Time `json:"ts"`
}

func (a AnchorRequest) GetMessageDeduplicationId() *string {
	dedupId := fmt.Sprintf("%s.%s", a.StreamId, a.Cid)
	return &dedupId
}

func (a AnchorRequest) GetMessageGroupId() *string {
	return &a.StreamId
}

type ReadyRequest struct {
	StreamId string `json:"streamId"`
}

func (r ReadyRequest) GetMessageDeduplicationId() *string {
	return &r.StreamId
}

func (r ReadyRequest) GetMessageGroupId() *string {
	return &r.StreamId
}
