package models

const DbDateFormat = "2006-01-02 15:04:05.000000"
const DbLoadLimit = 100

type CheckpointType string

const (
	CheckpointType_Poll CheckpointType = "poll"
)

type Checkpoint struct {
	Name  string `dynamodbav:"name"`
	Value string `dynamodbav:"value"`
}

type StreamCid struct {
	Id  string `dynamodbav:"id"`
	Cid string `dynamodbav:"cid"`
	// Filled in later
	StreamType *StreamType `dynamodbav:"stp,omitempty"` // stream type
	Controller *string     `dynamodbav:"ctl,omitempty"` // controller
	Family     *string     `dynamodbav:"fam,omitempty"` // family
	Loaded     *bool       `dynamodbav:"lod,omitempty"` // loading status
	CommitType *CommitType `dynamodbav:"ctp,omitempty"` // commit type
	Position   *int        `dynamodbav:"pos,omitempty"` // commit index
}
