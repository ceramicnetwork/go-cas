package ceramic

import (
	"bytes"

	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-varint"

	"github.com/ipfs/go-cid"

	"github.com/smrz2001/go-cas/models"
)

func MultiqueryId(query *models.CeramicQuery) string {
	buf := bytes.Buffer{}
	mqId := ""
	// If the genesis CID is present, we're trying to find a missing CID, otherwise we're doing a plain stream query.
	if query.GenesisCid != nil {
		buf.Write(varint.ToUvarint(206))
		buf.Write(varint.ToUvarint(uint64(*query.StreamType)))
		genesisCid, _ := cid.Parse(query.GenesisCid)
		commitCid, _ := cid.Parse(query.Cid)
		buf.Write(genesisCid.Bytes())
		buf.Write(commitCid.Bytes())
		mqId, _ = multibase.Encode(multibase.Base36, buf.Bytes())
	} else {
		mqId = query.StreamId
	}
	return mqId
}
