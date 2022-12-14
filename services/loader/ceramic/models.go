package ceramic

import (
	"time"

	"github.com/smrz2001/go-cas/models"
)

const CeramicServerTimeout = 60 * time.Second
const CeramicClientTimeout = 70 * time.Second

type CommitState struct {
	Cid  string            `json:"cid"`
	Type models.CommitType `json:"type"`
}

type StreamMetadata struct {
	Controllers []string `json:"controllers"`
	Family      *string  `json:"family,omitempty"`
}

type StreamState struct {
	Id       string            `json:"-"`
	Type     models.StreamType `json:"type"`
	Metadata StreamMetadata    `json:"metadata"`
	Log      []CommitState     `json:"log"`
}

type Stream struct {
	StreamId string      `json:"streamId"`
	State    StreamState `json:"state"`
}

type CidLookup struct {
	StreamId   string
	GenesisCid string
	Cid        string
	StreamType models.StreamType
}

type CidLookupResult struct {
	StreamState *StreamState
	Anchor      bool
	CidFound    bool
}
