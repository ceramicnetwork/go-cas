package models

import "time"

const CeramicTimeout = 1 * time.Minute

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
	Id       string         `json:"-"`
	Type     StreamType     `json:"type"`
	Metadata StreamMetadata `json:"metadata"`
	Log      []CommitState  `json:"log"`
}

type Stream struct {
	StreamId string      `json:"streamId"`
	State    StreamState `json:"state"`
}

type CeramicQuery struct {
	StreamId   string
	Cid        string
	GenesisCid *string
	StreamType *StreamType
}

type CeramicQueryResult struct {
	StreamState *StreamState
	Anchor      bool
	CidFound    bool
}
