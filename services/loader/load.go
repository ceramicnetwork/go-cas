package loader

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/smrz2001/go-cas"
	"github.com/smrz2001/go-cas/db"
	"github.com/smrz2001/go-cas/models"
	"github.com/smrz2001/go-cas/queue"
	"github.com/smrz2001/go-cas/queue/messages"
	"github.com/smrz2001/go-cas/services/loader/ceramic"
)

type CeramicLoader struct {
	cidLoader *ceramic.CidLoader
	stateDb   *db.StateDatabase
	requestQ  *queue.Queue[*messages.AnchorRequest]
	readyQ    *queue.Queue[*messages.ReadyRequest]
	queryCh   chan *queryContext
	queryWg   *sync.WaitGroup
}

type queryContext struct {
	query       *ceramic.CidQuery
	queryResult *ceramic.CidQueryResult
	rxId        *string
}

// TODO: Initialize with server context and use that for batch executor
func NewCeramicLoader() *CeramicLoader {
	cfg, err := cas.AwsConfig()
	if err != nil {
		log.Fatalf("newCeramicLoader: error creating aws cfg: %v", err)
	}
	return &CeramicLoader{
		ceramic.NewCidLoader(),
		db.NewStateDb(cfg),
		queue.NewQueue[*messages.AnchorRequest](cfg, string(queue.QueueType_Request)),
		queue.NewQueue[*messages.ReadyRequest](cfg, string(queue.QueueType_Ready)),
		// This is intentionally an unbuffered channel. It will be used to fan-out anchor request batches dequeued from
		// SQS, pass them through a Ceramic multiquery batch executor, which is in turn backed by a rate limiter. SQS
		// consumption will be controlled and so we should never have an uncontrolled number of records in the channel.
		make(chan *queryContext),
		new(sync.WaitGroup),
	}
}

func (l CeramicLoader) Load() {
	serverCtx := context.Background()

	// Start the worker
	l.startWorker(serverCtx)

	for {
		// TODO: This is where go-sqs can be used for fetching and processing multiple batches in parallel
		if anchorReqs, err := l.requestQ.Dequeue(serverCtx); err != nil {
			log.Printf("load: error polling batch from request queue: %v", err)
		} else if len(anchorReqs) > 0 {
			// TODO: Using go-sqs here would gracefully scale up the number of goroutines spawned for batch requests,
			// and would also result in fuller Ceramic multiquery requests, which is desirable.
			for _, anchorReq := range anchorReqs {
				l.queryCh <- &queryContext{
					query: &ceramic.CidQuery{
						StreamId: anchorReq.Body.StreamId,
						Cid:      anchorReq.Body.Cid,
					},
					rxId: anchorReq.ReceiptHandle,
				}
			}
		}
		// Sleep even if we had errors so that we don't get stuck in a tight loop
		time.Sleep(models.DefaultTick)
	}
}

func (l CeramicLoader) startWorker(ctx context.Context) {
	go func() {
		for {
			go l.processQuery(ctx, <-l.queryCh)
		}
	}()
}

func (l CeramicLoader) processQuery(ctx context.Context, queryCtx *queryContext) {
	streamId := queryCtx.query.StreamId
	cid := queryCtx.query.Cid
	queryResult, err := l.cidLoader.Multiquery(ctx, queryCtx.query)
	if err != nil {
		log.Printf("processStreams: error loading stream=%s, cid=%s from ceramic: %v", streamId, cid, err)
		return
	}
	// We found commits we haven't encountered before - make sure we anchor this stream, even if we didn't find the
	// specific commit requested.
	if queryResult.Anchor {
		if err = l.enqueueReadyRequest(); err != nil {
			// Don't delete the anchor request from the queue if this fails. If a ready request was not sent after the
			// previous (stream) query, it's possible we might not send a ready request for this stream/CID at all.
			// Continuing from here could cause the request to be reprocessed but that's ok.
			return
		}
	}
	if !queryResult.CidFound {
		// We might have already decided to anchor this stream but if we didn't find the CID we were looking for, send a
		// multiquery to see if we can find newer commits. We don't have to hold up anchoring for this stream to query
		// the missing commit if we already have *some* newer commits to anchor.
		if queryResult.StreamState != nil {
			// Add the genesis CID and stream type to the query so that we can construct a CID-specific multiquery
			queryCtx.query.GenesisCid = &queryResult.StreamState.Log[0].Cid
			queryCtx.query.StreamType = &queryResult.StreamState.Type
			l.queryCh <- &queryContext{
				query:       queryCtx.query,
				rxId:        queryCtx.rxId,
				queryResult: &queryResult,
			}
			// Return from here without deleting the message from SQS
			return
		} else {
			// TODO: Also post to Failure queue unless this is a permanent failure like CACAO expiration or rejection via conflict resolution
			// It's ok to fall through and delete the anchor request from the queue even if we couldn't mark the entry
			// as irretrievable in the state DB. In the worst case, if we get *another* request for this CID and try to
			// look it up again.
			l.markCidNotFound(streamId, cid)
		}
	}
	// Delete the anchor request from the queue if we couldn't find the stream at all, or if we did find it and were
	// able to mark it ready for anchoring.
	if _, err = l.requestQ.Delete(ctx, *queryCtx.rxId); err != nil {
		// It's ok if the anchor request couldn't get deleted, we'll just process it again. This will be infrequent
		// enough that it won't be a burden on the Ceramic node, and will also likely get deduped via SQS or our batcher
		// if we end up with multiple ready requests.
	}
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
