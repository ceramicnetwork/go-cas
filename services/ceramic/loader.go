package ceramic

import (
	"context"
	"time"

	"github.com/ceramicnetwork/go-cas/models"
)

type Loader struct {
	client  models.CeramicClient
	stateDb models.StateRepository
}

func NewCeramicLoader(client models.CeramicClient, stateDb models.StateRepository) *Loader {
	return &Loader{client: client, stateDb: stateDb}
}

func (l Loader) Query(ctx context.Context, query *models.CeramicQuery) (*models.CeramicQueryResult, error) {
	if streamState, err := l.client.Query(ctx, query); err != nil {
		return nil, err
	} else if streamState != nil {
		if queryResult, err := l.processStream(streamState, query.Cid); err != nil {
			return nil, err
		} else {
			return queryResult, nil
		}
	} else {
		return &models.CeramicQueryResult{}, nil
	}
}

func (l Loader) processStream(streamState *models.StreamState, cid string) (*models.CeramicQueryResult, error) {
	// Get the latest CID for this stream from the state DB
	pos := -1
	if latestStreamCid, err := l.stateDb.GetStreamTip(streamState.Id); err != nil {
		return nil, err
	} else if latestStreamCid != nil { // stream has been loaded in the past
		// Note the position of the latest entry so that we can use it with the loaded stream log
		pos = *latestStreamCid.Position
	}
	// We found commits we haven't encountered before - make sure we anchor this stream, even if we don't find the
	// specific commit requested.
	doAnchor := len(streamState.Log) > (pos + 1)
	cidFound := false
	for idx := 0; idx < len(streamState.Log); idx++ {
		if streamState.Log[idx].Cid == cid {
			cidFound = true
		}
		// Write all new CIDs to the state DB
		if idx > pos {
			if err := l.storeStreamCid(streamState, idx); err != nil {
				return nil, err
			}
		}
	}
	return &models.CeramicQueryResult{StreamState: streamState, Anchor: doAnchor, CidFound: cidFound}, nil
}

func (l Loader) storeStreamCid(streamState *models.StreamState, idx int) error {
	streamCid := models.StreamCid{
		StreamId:   streamState.Id,
		Cid:        streamState.Log[idx].Cid,
		CommitType: &streamState.Log[idx].Type,
		Position:   &idx,
		Timestamp:  time.Now().UTC(),
	}
	// Only store metadata in genesis record
	if idx == 0 {
		streamCid.StreamType = &streamState.Type
		streamCid.Controller = &streamState.Metadata.Controllers[0]
		streamCid.Family = streamState.Metadata.Family
	}
	if err := l.stateDb.UpdateCid(&streamCid); err != nil {
		return err
	}
	return nil
}
