package ceramic

import (
	"context"
	"log"
	"time"

	"github.com/abevier/tsk/batch"
	"github.com/abevier/tsk/futures"
	"github.com/abevier/tsk/ratelimiter"
	"github.com/abevier/tsk/results"

	"github.com/smrz2001/go-cas"
	"github.com/smrz2001/go-cas/db"
	"github.com/smrz2001/go-cas/models"
)

type CidLoader struct {
	client        *CeramicClient
	stateDb       *db.StateDatabase
	batchExecutor *batch.BatchExecutor[*CidQuery, CidQueryResult]
	rateLimiter   *ratelimiter.RateLimiter[[]*CidQuery, []results.Result[CidQueryResult]]
}

// TODO: Initialize with server context and use that for batch executor
func NewCidLoader() *CidLoader {
	cfg, err := cas.AwsConfig()
	if err != nil {
		log.Fatalf("error creating aws cfg: %v", err)
	}
	cidLoader := CidLoader{
		client:  NewCeramicClient(),
		stateDb: db.NewStateDb(cfg),
	}
	beOpts := batch.BatchOpts{
		MaxSize:   models.DefaultBatchMaxDepth,
		MaxLinger: models.DefaultBatchMaxLinger,
	}
	rlOpts := ratelimiter.RateLimiterOpts{
		Limit:             models.DefaultRateLimit,
		Burst:             models.DefaultRateLimit,
		FullQueueStrategy: ratelimiter.BlockWhenFull,
	}
	cidLoader.batchExecutor = batch.NewExecutor[*CidQuery, CidQueryResult](beOpts, func(queries []*CidQuery) ([]results.Result[CidQueryResult], error) {
		return cidLoader.rateLimiter.Submit(context.Background(), queries)
	})
	cidLoader.rateLimiter = ratelimiter.New(rlOpts, cidLoader.multiquery)
	return &cidLoader
}

func (cl CidLoader) MultiqueryF(query *CidQuery) *futures.Future[CidQueryResult] {
	return cl.batchExecutor.SubmitF(query)
}

func (cl CidLoader) Multiquery(ctx context.Context, query *CidQuery) (CidQueryResult, error) {
	return cl.MultiqueryF(query).Get(ctx)
}

func (cl CidLoader) query(ctx context.Context, query CidQuery) (results.Result[CidQueryResult], error) {
	// Load the stream
	if streamState, err := cl.client.query(ctx, query.StreamId); err != nil {
		log.Printf("loadCid: error submitting task: %v", err)
		return results.Result[CidQueryResult]{}, err
	} else if queryResult, err := cl.processStream(streamState, query.Cid); err != nil {
		return results.Result[CidQueryResult]{}, err
	} else {
		return results.New[CidQueryResult](queryResult, nil), nil
	}
}

func (cl CidLoader) multiquery(ctx context.Context, queries []*CidQuery) ([]results.Result[CidQueryResult], error) {
	queryResults := make([]results.Result[CidQueryResult], len(queries))
	if mqResp, err := cl.client.multiquery(ctx, queries); err != nil {
		return nil, err
	} else {
		for idx, query := range queries {
			queryResult := CidQueryResult{}
			if streamState, found := mqResp[query.mqId()]; found {
				streamState.Id = query.StreamId
				queryResult, err = cl.processStream(streamState, query.Cid)
				if err != nil {
					return nil, err
				}
			}
			queryResults[idx] = results.New[CidQueryResult](queryResult, nil)
		}
	}
	return queryResults, nil
}

func (cl CidLoader) processStream(streamState *StreamState, cid string) (CidQueryResult, error) {
	// Get the latest CID for this stream from the state DB
	pos := -1
	if latestStreamCid, err := cl.stateDb.GetLatestCid(streamState.Id); err != nil {
		log.Printf("loadCid: error loading stream=%s from db: %v", streamState.Id, err)
		return CidQueryResult{}, err
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
			if err := cl.storeStreamCid(streamState, idx); err != nil {
				return CidQueryResult{}, err
			}
		}
	}
	return CidQueryResult{streamState, doAnchor, cidFound}, nil
}

func (cl CidLoader) storeStreamCid(streamState *StreamState, idx int) error {
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
	if err := cl.stateDb.UpdateCid(&streamCid); err != nil {
		log.Printf("storeStreamCid: error writing stream=%s, cid=%s to db: %v", streamState.Id, streamState.Log[idx].Cid, err)
		return err
	}
	return nil
}
