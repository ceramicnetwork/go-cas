package models

import "time"

const MaxOutstandingMultiQueries = 10
const MaxStreamsPerMultiQuery = 10
const MultiQueryTimeout = time.Minute

type CommitType uint8

const (
	CommitType_Genesis CommitType = iota
	CommitType_Signed
	CommitType_Anchor
)

type StreamType uint8

const (
	StreamType_Tile StreamType = iota
)

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
