package loader

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/abevier/tsk/batch"
	"github.com/abevier/tsk/results"
	"github.com/abevier/tsk/taskqueue"

	"github.com/smrz2001/go-cas"
	"github.com/smrz2001/go-cas/db"
	"github.com/smrz2001/go-cas/models"
	"github.com/smrz2001/go-cas/queue"
	"github.com/smrz2001/go-cas/queue/messages"
	"github.com/smrz2001/go-cas/services/loader/ceramic"
)

type CeramicLoader struct {
	cidLoader         *ceramic.CidLoader
	stateDb           *db.StateDatabase
	requestQ          *queue.Queue[*messages.AnchorRequest]
	readyQ            *queue.Queue[*messages.ReadyRequest]
	lookupTaskQ       *taskqueue.TaskQueue[ceramic.CidLookup, ceramic.CidLookupResult]
	missingCidBatchEx *batch.BatchExecutor[ceramic.CidLookup, ceramic.CidLookupResult]
}

func NewCeramicLoader() *CeramicLoader {
	cfg, err := cas.AwsConfig()
	if err != nil {
		log.Fatalf("newCeramicLoader: error creating aws cfg: %v", err)
	}
	cidLoader := ceramic.NewCidLoader()
	// Stream lookup task queue
	tqOpts := taskqueue.TaskQueueOpts{
		MaxWorkers:        models.TaskQueueMaxWorkers,
		MaxQueueDepth:     models.TaskQueueMaxQueueDepth,
		FullQueueBehavior: taskqueue.BlockWhenFull,
	}
	tqRun := func(ctx context.Context, lookup ceramic.CidLookup) (ceramic.CidLookupResult, error) {
		return cidLoader.LoadCid(ctx, lookup)
	}
	// Missing CID lookup batch executor
	beOpts := batch.BatchOpts{
		MaxSize:   models.BatchMaxDepth,
		MaxLinger: models.BatchMaxLinger,
	}
	beRun := func(lookups []ceramic.CidLookup) ([]results.Result[ceramic.CidLookupResult], error) {
		// TODO: Take context from caller?
		lookupResults, err := cidLoader.LoadMissingCids(context.Background(), lookups)
		batchExResults := make([]results.Result[ceramic.CidLookupResult], len(lookupResults))
		for idx, lookupResult := range lookupResults {
			batchExResults[idx] = results.New[ceramic.CidLookupResult](lookupResult, nil)
		}
		return batchExResults, err
	}
	return &CeramicLoader{
		cidLoader,
		db.NewStateDb(cfg),
		queue.NewQueue[*messages.AnchorRequest](cfg, string(queue.QueueType_Request)),
		queue.NewQueue[*messages.ReadyRequest](cfg, string(queue.QueueType_Ready)),
		taskqueue.NewTaskQueue[ceramic.CidLookup, ceramic.CidLookupResult](tqOpts, tqRun),
		batch.NewExecutor[ceramic.CidLookup, ceramic.CidLookupResult](beOpts, beRun),
	}
}

func (l CeramicLoader) Load() {
	// 1. Read a batch of messages from the queue.
	// 2. Make stream load request.
	// 3. Make multiquery for CIDs not found.
	// 4. Persist loading result to the state DB, along with all commits discovered.
	// 5. Post stream ID to Ready queue.
	// 6. Remove request from the queue.
	for {
		// Read a batch of messages from the Request queue
		if reqMsgs, err := l.requestQ.Dequeue(); err != nil {
			log.Printf("load: error polling batch from request queue: %v", err)
		} else if len(reqMsgs) > 0 {
			func() {
				batchCtx, batchCancel := context.WithTimeout(context.Background(), models.MaxBatchProcessingTime)
				defer batchCancel()

				wg := sync.WaitGroup{}
				wg.Add(len(reqMsgs))

				// For each message in the batch, make a request to Ceramic to load the stream.
				for _, reqMsg := range reqMsgs {
					streamId := reqMsg.Body.StreamId
					cid := reqMsg.Body.Cid
					rxId := *reqMsg.ReceiptHandle
					go func() {
						defer wg.Done()

						if lookupResult, err := l.lookupTaskQ.Submit(batchCtx, ceramic.CidLookup{StreamId: streamId, Cid: cid}); err != nil {
							log.Printf("load: error loading stream=%s, cid=%s from ceramic: %v", streamId, cid, err)
							// TODO: Post to Failure queue
						} else {
							// We might have already decided to anchor this stream but if we didn't find the CID we were
							// looking for, send a multiquery to see if we can find newer commits. We don't have to hold
							// up anchoring for this stream in order to lookup the missing commit if we have newer
							// commits to anchor anyway.
							doAnchor := lookupResult.Anchor
							if !lookupResult.CidFound {
								// We can assume that if we reached here, we at least found the stream, which means we
								// also have a genesis commit that we can use.
								missingCidLookup := ceramic.CidLookup{
									StreamId:   streamId,
									GenesisCid: lookupResult.StreamState.Log[0].Cid,
									Cid:        cid,
									StreamType: lookupResult.StreamState.Type,
								}
								// TODO: If we find the commit we were looking for, use the updated stream state to
								// decide whether to anchor this stream
								batchResult, err := l.missingCidBatchEx.Submit(batchCtx, missingCidLookup)
								if err != nil {
									log.Printf("load: error loading stream=%s, cid=%s from ceramic: %v", streamId, cid, err)
									// TODO: Post to Failure queue
								} else if batchResult.Anchor {
									doAnchor = batchResult.Anchor
								}
							}
							// We found commits we haven't encountered before - make sure we anchor this stream, even if
							// we didn't find the specific commit requested.
							if doAnchor {
								// TODO: Post the stream to the Ready queue
								//if _, err = l.readyQ.Enqueue(&messages.ReadyRequest{StreamId: reqMsg.Body.StreamId}); err != nil {
								//	log.Printf("load: error queueing ready request for stream=%s, cid=%s: %v", reqMsg.Body.StreamId, reqMsg.Body.Cid, err)
								//	// TODO: Post to Failure queue or just not ACK from Request queue?
								//}
							}
						}
						// TODO: Batch deletion requests
						// Delete the message from the queue
						if err = l.requestQ.Ack(rxId); err != nil {
							log.Printf("load: error acknowledging request for stream=%s, cid=%s, rxId=%s: %v", streamId, cid, rxId, err)
						}
					}()
				}
				// TODO: More sophisticated batch processing?
				// Wait for a batch to be processed before pulling the next one
				wg.Wait()
			}()
			// Sleep even if we had errors so that we don't get stuck in a tight loop
			time.Sleep(models.DefaultTick)
		}
	}
}
