package messages

type ReadyRequest struct {
	StreamId string `json:"streamId"`
}

func (r ReadyRequest) GetMessageDeduplicationId() *string {
	return &r.StreamId
}

func (r ReadyRequest) GetMessageGroupId() *string {
	return &r.StreamId
}
