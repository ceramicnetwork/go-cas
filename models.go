package cas

import (
	"time"

	"github.com/google/uuid"
)

const DefaultTick = 30 * time.Second
const DefaultHttpWaitTime = 10 * time.Second

const MaxOutstandingRequests = 100
const MaxNumTaskWorkers = 4

const DbDateFormat = "2006-01-02 15:04:05.000000"
const DbLoadLimit = 1000

const MaxOutstandingMultiQueries = 10
const MaxStreamsPerMultiQuery = 10
const MultiQueryTimeout = time.Minute
const MaxBatchLinger = time.Minute

type CommitType uint8

const (
	CommitType_Genesis CommitType = iota
	CommitType_Signed
	CommitType_Anchor
)

type StreamType uint8

const (
	CommitType_Tile StreamType = iota
)

type RequestStatus string

const (
	RequestStatus_Pending    RequestStatus = "0"
	RequestStatus_Processing               = "1"
	RequestStatus_Completed                = "2"
	RequestStatus_Failed                   = "3"
	RequestStatus_Ready                    = "4"
)

type CheckpointType string

const (
	CheckpointType_Poll CheckpointType = "poll"
)

type AnchorRequest struct {
	Id        uuid.UUID `json:"reqId"`
	StreamId  string    `json:"streamId"`
	Cid       string    `json:"cid"`
	CreatedAt time.Time `json:"ts"`
	// Omitted (only used for reading from Postgres)
	Status    int       `json:"-"`
	Message   string    `json:"-"`
	UpdatedAt time.Time `json:"-"`
	Pinned    bool      `json:"-"`
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

type CommitState struct {
	Cid  string     `json:"cid"`
	Type CommitType `json:"type"`
}

type StreamMetadata struct {
	Controllers []string `json:"controllers"`
	Family      *string  `json:"family,omitempty"`
}

type StreamState struct {
	Type     StreamType     `json:"type"`
	Metadata StreamMetadata `json:"metadata"`
	Log      []CommitState  `json:"log"`
}

type Checkpoint struct {
	Name  string `dynamodbav:"name"`
	Value string `dynamodbav:"value"`
}

type Database interface {
	GetCheckpoint(CheckpointType) (string, error)
	UpdateCheckpoint(CheckpointType, string) (bool, error)
	GetCid(string, string) (*StreamCid, error)
	GetLatestCid(string) (*StreamCid, error)
	CreateCid(*StreamCid) (bool, error)
	UpdateCid(*StreamCid) error
}

type Queue interface {
	Enqueue(interface{}) (string, error)
	Dequeue() ([]interface{}, error)
}
