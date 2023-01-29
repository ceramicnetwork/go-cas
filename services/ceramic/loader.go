package ceramic

import (
	"context"
	"time"

	"github.com/abevier/tsk/batch"
	"github.com/abevier/tsk/ratelimiter"
	"github.com/abevier/tsk/results"

	"github.com/smrz2001/go-cas/models"
)

type Loader struct {
	client      models.CeramicClient
	stateDb     models.StateRepository
	rateLimiter *ratelimiter.RateLimiter[*models.CeramicQuery, *models.CeramicQueryResult]
	mqBatcher   *batch.Executor[*models.CeramicQuery, *models.CeramicQueryResult]
}

func NewCeramicLoader(client models.CeramicClient, stateDb models.StateRepository) *Loader {
	loader := Loader{client: client, stateDb: stateDb}
	beOpts := batch.Opts{MaxSize: models.DefaultBatchMaxDepth, MaxLinger: models.DefaultBatchMaxLinger}
	rlOpts := ratelimiter.Opts{
		Limit:             models.DefaultRateLimit,
		Burst:             models.DefaultRateLimit,
		MaxQueueDepth:     models.DefaultQueueDepthLimit,
		FullQueueStrategy: ratelimiter.BlockWhenFull,
	}
	// Put the rate limiter in front of the batch executor so that the former can be used transparently for both queries
	// and multiqueries going through Ceramic. Multiqueries will get further paced because of going through the batcher
	// but that's ok.
	loader.rateLimiter = ratelimiter.New(rlOpts, func(ctx context.Context, query *models.CeramicQuery) (*models.CeramicQueryResult, error) {
		// Use the presence or absence of the genesis CID to decide whether to perform a normal Ceramic stream query, or
		// a Ceramic multiquery for a missing commit.
		if (query.GenesisCid == nil) || (len(*query.GenesisCid) == 0) {
			return loader.query(ctx, query)
		}
		return loader.mqBatcher.Submit(ctx, query)
	})
	// TODO: Use better context
	loader.mqBatcher = batch.New[*models.CeramicQuery, *models.CeramicQueryResult](beOpts, func(queries []*models.CeramicQuery) ([]results.Result[*models.CeramicQueryResult], error) {
		return loader.multiquery(context.Background(), queries)
	})
	return &loader
}

func (l Loader) Query(ctx context.Context, query *models.CeramicQuery) (*models.CeramicQueryResult, error) {
	return l.rateLimiter.Submit(ctx, query)
}

func (l Loader) query(ctx context.Context, query *models.CeramicQuery) (*models.CeramicQueryResult, error) {
	if streamState, err := l.client.Query(ctx, query.StreamId); err != nil {
		return nil, err
	} else if queryResult, err := l.processStream(streamState, query.Cid); err != nil {
		return nil, err
	} else {
		return queryResult, nil
	}
}

func (l Loader) multiquery(ctx context.Context, queries []*models.CeramicQuery) ([]results.Result[*models.CeramicQueryResult], error) {
	queryResults := make([]results.Result[*models.CeramicQueryResult], len(queries))
	if mqResp, err := l.client.Multiquery(ctx, queries); err != nil {
		return nil, err
	} else {
		// Fan the multiquery results back out
		for idx, query := range queries {
			var queryResult *models.CeramicQueryResult
			if streamState, found := mqResp[l.client.MultiqueryId(query)]; found {
				streamState.Id = query.StreamId
				queryResult, err = l.processStream(streamState, query.Cid)
				if err != nil {
					return nil, err
				}
			} else {
				queryResult = &models.CeramicQueryResult{}
			}
			queryResults[idx] = results.New[*models.CeramicQueryResult](queryResult, nil)
		}
	}
	return queryResults, nil
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
