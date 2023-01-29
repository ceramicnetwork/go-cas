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
	StreamId   string      `dynamodbav:"id"`
	Cid        string      `dynamodbav:"cid"`
	Timestamp  time.Time   `dynamodbav:"ts,unixtime"` // can be used for TTL
	StreamType *StreamType `dynamodbav:"stp,omitempty"`
	Controller *string     `dynamodbav:"ctl,omitempty"`
	Family     *string     `dynamodbav:"fam,omitempty"`
	CommitType *CommitType `dynamodbav:"ctp,omitempty"`
	Loaded     *bool       `dynamodbav:"lod,omitempty"`
	Position   *int        `dynamodbav:"pos,omitempty"` // commit index in stream log
}
