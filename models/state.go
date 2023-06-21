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
	StreamId  string    `dynamodbav:"id"`           // hash key
	Cid       string    `dynamodbav:"cid"`          // range key
	CreatedAt time.Time `dynamodbav:"crt,unixtime"` // can be used as TTL
}

type StreamTip struct {
	StreamId  string    `dynamodbav:"id"`  // hash key
	Origin    string    `dynamodbav:"org"` // range key
	Id        string    `dynamodbav:"rid"`
	Cid       string    `dynamodbav:"cid"`
	Timestamp time.Time `dynamodbav:"ts"`
	CreatedAt time.Time `dynamodbav:"crt,unixtime"` // can be used as TTL
}
