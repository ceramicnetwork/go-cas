package models

import "time"

type CheckpointType string

const (
	CheckpointType_RequestPoll CheckpointType = "rpoll"
	CheckpointType_FailurePoll CheckpointType = "fpoll"
)

type Checkpoint struct {
	Name  string `dynamodbav:"name"`
	Value string `dynamodbav:"value"`
}

type StreamCid struct {
	StreamId  string     `dynamodbav:"id"`
	Cid       string     `dynamodbav:"cid"`
	Timestamp time.Time  `dynamodbav:"ts,unixtime"` // can be used for TTL
	AnchorTs  *time.Time `dynamodbav:"anc,unixtime,omitempty"`
}
