package ceramic

import (
	"bytes"
	"time"

	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-varint"

	"github.com/ipfs/go-cid"

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

type CidQuery struct {
	StreamId   string
	Cid        string
	GenesisCid *string
	StreamType *models.StreamType
}

func (q CidQuery) mqId() string {
	buf := bytes.Buffer{}
	mqId := ""
	// If the genesis CID is present, we're trying to find a missing CID, otherwise we're doing a plain stream query.
	if q.GenesisCid != nil {
		buf.Write(varint.ToUvarint(206))
		buf.Write(varint.ToUvarint(uint64(*q.StreamType)))
		genesisCid, _ := cid.Parse(q.GenesisCid)
		commitCid, _ := cid.Parse(q.Cid)
		buf.Write(genesisCid.Bytes())
		buf.Write(commitCid.Bytes())
		mqId, _ = multibase.Encode(multibase.Base36, buf.Bytes())
	} else {
		mqId = q.StreamId
	}
	return mqId
}

type CidQueryResult struct {
	StreamState *StreamState
	Anchor      bool
	CidFound    bool
}
