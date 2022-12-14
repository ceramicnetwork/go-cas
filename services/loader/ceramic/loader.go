package ceramic

import (
	"context"
	"github.com/smrz2001/go-cas"
	"github.com/smrz2001/go-cas/db"
	"github.com/smrz2001/go-cas/models"
	"log"
	"time"
)

type CidLoader struct {
	client  *CeramicClient
	stateDb *db.StateDatabase
}

func NewCidLoader() *CidLoader {
	cfg, err := cas.AwsConfig()
	if err != nil {
		log.Fatalf("error creating aws cfg: %v", err)
	}
	ceramicClient := NewCeramicClient()
	return &CidLoader{
		ceramicClient,
		db.NewStateDb(cfg),
	}
}

// LoadCid does the following things:
// - Try to load the stream and find the CID. Once CAS w/o Ceramic is implemented, the loading step won't be necessary,
//   and we can decide what to do with the stream using just the state DB.
// - If found, return all CIDs from the stream so that the caller can decide if/when/how to store them.
//   - This would resolve the future
// - If not found, send a multi-query to force the Ceramic node to reload the stream from the network.
//   - This can be also be put in a batch executor so we can combine multiple CID requests into a single multiquery
// - If we get more information in the mq response, return that to the caller.
//   - This would resolve the future
// - If not found, this would fail the future and result in a message being posted to the failure queue.
//   - For now, till the batcher is implemented, this would just leave a non-loaded entry in the state DB that we can
//     come back to later.
func (l CidLoader) LoadCid(ctx context.Context, lookup CidLookup) (CidLookupResult, error) {
	// Check if the stream/CID has already been looked up and stored in the state DB
	streamCid, err := l.stateDb.GetCid(lookup.StreamId, lookup.Cid)
	if err != nil {
		log.Printf("loadCid: error loading stream=%s, cid=%s from db: %v", lookup.StreamId, lookup.Cid, err)
		return CidLookupResult{nil, false, false}, err
	} else if streamCid == nil {
		qCtx, qCancel := context.WithTimeout(ctx, CeramicClientTimeout)
		defer qCancel()

		// Load the stream
		streamState, err := l.client.query(qCtx, lookup.StreamId)
		if err != nil {
			log.Printf("loadCid: error submitting task: %v", err)
			return CidLookupResult{nil, false, false}, err
		}
		return l.processStream(streamState, lookup.Cid)
	}
	// Return CID found = true but anchor needed = false since we already have this CID in the DB and must have already
	// processed it and its anchor request.
	log.Printf("processStream: stream=%s, cid=%s found in db", lookup.StreamId, lookup.Cid)
	return CidLookupResult{nil, false, true}, nil
}

func (l CidLoader) LoadMissingCids(ctx context.Context, lookups []CidLookup) ([]CidLookupResult, error) {
	mqCtx, mqCancel := context.WithTimeout(ctx, CeramicClientTimeout)
	defer mqCancel()

	// Load the stream
	streamStates, err := l.client.multiquery(mqCtx, lookups)
	if err != nil {
		log.Printf("loadMissingCids: error submitting task: %v", err)
		return nil, err
	}
	lookupResults := make([]CidLookupResult, len(lookups))
	for idx, streamState := range streamStates {
		lookupResult, err := l.processStream(streamState, lookups[idx].Cid)
		if err != nil {
			return nil, err
		}
		lookupResults[idx] = lookupResult
	}
	return lookupResults, nil
}

func (l CidLoader) processStream(streamState *StreamState, cid string) (CidLookupResult, error) {
	// Get the latest CID for this stream from the state DB
	pos := -1
	if latestStreamCid, err := l.stateDb.GetLatestCid(streamState.Id); err != nil {
		log.Printf("loadCid: error loading stream=%s from db: %v", streamState.Id, err)
		return CidLookupResult{nil, false, false}, err
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
				return CidLookupResult{nil, false, false}, err
			}
		}
	}
	return CidLookupResult{streamState, doAnchor, cidFound}, nil
}

func (l CidLoader) storeStreamCid(streamState *StreamState, idx int) error {
	streamCid := models.StreamCid{
		StreamId:   streamState.Id,
		Cid:        streamState.Log[idx].Cid,
		CommitType: &streamState.Log[idx].Type,
		Position:   &idx,
		// Can use this to audit how long it's been since a stream in the DB was anchored
		Timestamp: time.Now(),
	}
	// Only store metadata in genesis record
	if idx == 0 {
		streamCid.StreamType = &streamState.Type
		streamCid.Controller = &streamState.Metadata.Controllers[0]
		streamCid.Family = streamState.Metadata.Family
	}
	if err := l.stateDb.UpdateCid(&streamCid); err != nil {
		log.Printf("storeStreamCid: error writing stream=%s, cid=%s to db: %v", streamState.Id, streamState.Log[idx].Cid, err)
		return err
	}
	return nil
}
