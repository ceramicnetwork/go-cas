package loader

import (
	"context"
	"log"
	"time"

	"github.com/abevier/tsk/futures"

	"github.com/smrz2001/go-cas"
	"github.com/smrz2001/go-cas/db"
	"github.com/smrz2001/go-cas/models"
	"github.com/smrz2001/go-cas/queue"
	"github.com/smrz2001/go-cas/queue/messages"
	"github.com/smrz2001/go-cas/services/loader/ceramic"
)

// TODO: Asbtract pattern for "Service"? Going to need the state DB and ingress/egress (SQS) queues for the loading,
// batching, and failure handling services.
type CeramicLoader struct {
	cidLoader *ceramic.CidLoader
	stateDb   *db.StateDatabase
	requestQ  *queue.Queue[*messages.AnchorRequest]
	readyQ    *queue.Queue[*messages.ReadyRequest]
}

type queryContext struct {
	query       *ceramic.CidQuery
	queryResult *ceramic.CidQueryResult
	rxId        *string
	future      *futures.Future[ceramic.CidQueryResult]
}

func NewCeramicLoader() *CeramicLoader {
	cfg, err := cas.AwsConfig()
	if err != nil {
		log.Fatalf("newCeramicLoader: error creating aws cfg: %v", err)
	}
	return &CeramicLoader{
		cidLoader: ceramic.NewCidLoader(),
		stateDb:   db.NewStateDb(cfg),
		requestQ:  queue.NewQueue[*messages.AnchorRequest](cfg, string(queue.QueueType_Request)),
		readyQ:    queue.NewQueue[*messages.ReadyRequest](cfg, string(queue.QueueType_Ready)),
	}
}

func (l CeramicLoader) Load() {
	for {
		// TODO: Use go-sqs to scale up/down
		if anchorReqs, err := l.requestQ.Dequeue(); err != nil {
			log.Printf("load: error polling batch from request queue: %v", err)
		} else if len(anchorReqs) > 0 {
			// This call will block until it's done
			l.processBatch(anchorReqs)
		}
		// Sleep even if we had errors so that we don't get stuck in a tight loop
		time.Sleep(models.DefaultTick)
	}
}

func (l CeramicLoader) processBatch(anchorReqs []*queue.QueueMessage[*messages.AnchorRequest]) {
	ctx, cancel := context.WithTimeout(context.Background(), models.MaxBatchProcessingTime)
	defer cancel()

	// Prepare the query contexts for the initial stream queries
	queries := make([]*queryContext, 0)
	for _, anchorReq := range anchorReqs {
		streamId := anchorReq.Body.StreamId
		cid := anchorReq.Body.Cid
		queries = append(queries, &queryContext{
			query:  &ceramic.CidQuery{StreamId: streamId, Cid: cid},
			rxId:   anchorReq.ReceiptHandle,
			future: nil,
		})
	}
	// Process stream queries
	missingCidQueries := l.processStreams(ctx, queries)
	// Process missing CID queries
	l.processStreams(ctx, missingCidQueries)
}

func (l CeramicLoader) processStreams(ctx context.Context, queryContexts []*queryContext) []*queryContext {
	missingCidQueries := make([]*queryContext, 0)
	// Prepare query futures first so that individual queries will get fanned in for the Ceramic multiquery
	for _, queryCtx := range queryContexts {
		streamId := queryCtx.query.StreamId
		cid := queryCtx.query.Cid
		// Check if the stream/CID has already been looked up and stored in the state DB
		if streamCid, err := l.stateDb.GetCid(streamId, cid); err != nil {
			log.Printf("processStreams: error loading stream=%s, cid=%s from db: %v", streamId, cid, err)
		} else if (streamCid == nil) || ((streamCid.Loaded != nil) && !*streamCid.Loaded) {
			queryCtx.future = l.cidLoader.MultiqueryF(queryCtx.query)
		} else {
			// Add a completed future if we found the stream/CID
			queryCtx.future = futures.New[ceramic.CidQueryResult]()
			queryCtx.future.Complete(ceramic.CidQueryResult{CidFound: true})
		}
	}
	for _, queryCtx := range queryContexts {
		// This line will block until the future gets resolved
		if queryResult, err := queryCtx.future.Get(ctx); err != nil {
			log.Printf("processStreams: error loading stream=%s, cid=%s from ceramic: %v", queryCtx.query.StreamId, queryCtx.query.Cid, err)
		} else {
			// We found commits we haven't encountered before - make sure we anchor this stream, even if we didn't find
			// the specific commit requested.
			if queryResult.Anchor {
				if err = l.enqueueReadyRequest(); err != nil {
					// Don't delete the anchor request from the queue if this fails. If a ready request was not sent
					// after the previous (stream) query, it's possible we might not send a ready request for this
					// stream/CID at all. Continuing from here could cause the request to be reprocessed but that's ok.
					continue
				}
			}
			if !queryResult.CidFound {
				// We might have already decided to anchor this stream but if we didn't find the CID we were looking
				// for, send a multiquery to see if we can find newer commits. We don't have to hold up anchoring
				// for this stream to query the missing commit if we already have *some* newer commits to anchor.
				if queryResult.StreamState != nil {
					// Add the genesis CID and stream type to the query so that we can construct a CID-specific
					// multiquery.
					queryCtx.query.GenesisCid = queryResult.StreamState.Log[0].Cid
					queryCtx.query.StreamType = queryResult.StreamState.Type
					missingCidQueries = append(missingCidQueries, &queryContext{
						query:       queryCtx.query,
						rxId:        queryCtx.rxId,
						queryResult: &queryResult,
					})
					continue
				} else {
					// TODO: Also post to Failure queue unless this is a permanent failure like CACAO expiration or rejection via conflict resolution
					// It's ok to fall through and delete the anchor request from the queue even if we couldn't mark the
					// entry as irretrievable in the state DB. In the worst case, if we get *another* request for this
					// CID and try to look it up again.
					l.markCidNotFound(queryCtx.query.StreamId, queryCtx.query.Cid)
				}
			}
			// Delete the anchor request from the queue if we couldn't find the stream at all, or if we did find it and
			// were able to mark it ready for anchoring.
			if _, err = l.requestQ.Delete(ctx, *queryCtx.rxId); err != nil {
				// It's ok if the anchor request couldn't get deleted, we'll just process it again. This will be
				// infrequent enough that it won't be a burden on the Ceramic node, and will also likely get deduped via
				// SQS or our batcher if we end up with multiple ready requests.
			}
		}
	}
	return missingCidQueries
}

func (l CeramicLoader) markCidNotFound(streamId, cid string) {
	// Mark the entry as irretrievable in the state DB
	streamCid := models.StreamCid{
		StreamId:  streamId,
		Cid:       cid,
		Timestamp: time.Now(),
		Loaded:    new(bool), // defaults to false
	}
	if err := l.stateDb.UpdateCid(&streamCid); err != nil {
		log.Printf("load: error writing stream=%s, cid=%s to db: %v", streamId, cid, err)
	}
}

func (l CeramicLoader) enqueueReadyRequest() error {
	// TODO: Post the stream to the Ready queue
	//if _, err = l.readyQ.Enqueue(&messages.ReadyRequest{StreamId: reqMsg.Body.StreamId}); err != nil {
	//	log.Printf("load: error queueing ready request for stream=%s, cid=%s: %v", reqMsg.Body.StreamId, reqMsg.Body.Cid, err)
	//}
	return nil
}
