package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/emirpasic/gods/sets/hashset"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/abevier/tsk/batch"
	"github.com/abevier/tsk/result"
	"github.com/abevier/tsk/taskqueue"

	"github.com/smrz2001/go-cas"
	"github.com/smrz2001/go-cas/aws"
)

// RequestCh is the "queue" for incoming requests
var RequestCh = make(chan cas.StreamCid, cas.MaxOutstandingRequests)

// CeramicUrl is the Ceramic node to hit for making multi-queries
var CeramicUrl = os.Getenv("CERAMIC_URL")

var tq *taskqueue.TaskQueue[[]cas.StreamCid, int]
var be *batch.BatchExecutor[cas.StreamCid, int]

var StreamCtr = 0
var CommitCtr = 0

// TODO: Keep LRU cache of streams -> latest CID?

func LoadStreamsFromCeramic() {
	cfg, err := aws.Config()
	if err != nil {
		log.Fatalf("error creating aws cfg: %v", err)
	}
	db := aws.NewDynamoDb(cfg)

	// Task queue for making Ceramic multi-queries
	tq = taskqueue.NewTaskQueue(taskqueue.TaskQueueOpts{
		MaxWorkers:        cas.MaxNumTaskWorkers,
		MaxQueueDepth:     cas.MaxOutstandingMultiQueries,
		FullQueueBehavior: taskqueue.BlockWhenFull,
	}, func(streamCids []cas.StreamCid) (int, error) {
		return ceramicMultiQuery(streamCids, db)
	})
	// Batch executor to collect incoming requests for Ceramic multi-queries
	be = batch.NewBatchExecutor(batch.BatchOpts{
		MaxSize:   cas.MaxStreamsPerMultiQuery,
		MaxLinger: cas.MaxBatchLinger,
	}, batchRequests)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case streamCid := <-RequestCh:
				{
					// TODO: What should we do with the array of errors if batch execution fails?
					wg.Add(1)
					go func() {
						defer wg.Done()

						_, err = be.Submit(context.Background(), streamCid)
						if err != nil {
							log.Printf("error submitting request: %v", err)
						}
					}()
				}
			default:
				// TODO
			}
		}
	}()
	wg.Wait()
}

func batchRequests(streamCids []cas.StreamCid) ([]result.Result[int], error) {
	if _, err := tq.Submit(context.TODO(), streamCids); err != nil {
		return nil, err
	}
	return []result.Result[int]{}, nil
}

func ceramicMultiQuery(streamCids []cas.StreamCid, db cas.Database) (int, error) {
	mqCtx, mqCancel := context.WithTimeout(context.TODO(), cas.MultiQueryTimeout)
	defer mqCancel()

	type streamQuery struct {
		StreamId string `json:"streamId"`
	}
	type multiQuery struct {
		Queries []streamQuery `json:"queries"`
	}

	mq := multiQuery{make([]streamQuery, 0, len(streamCids))}
	// Dedup stream IDs in batch
	dedupedQueries := hashset.New()
	for _, streamCid := range streamCids {
		if !dedupedQueries.Contains(streamCid.Id) {
			// Only make the Ceramic multi-query if we don't already have the stream or CID in the DB, or if it hasn't
			// been loaded yet.
			if dbStreamCid, err := db.GetCid(streamCid.Id, streamCid.Cid); err != nil {
				log.Printf("error fetching stream=%s, cid=%s from db: %v", streamCid.Id, streamCid.Cid, err)
				return -1, err
			} else if (dbStreamCid == nil) || (dbStreamCid.Loaded == nil) || !*dbStreamCid.Loaded {
				mq.Queries = append(mq.Queries, streamQuery{streamCid.Id})
				dedupedQueries.Add(streamCid.Id)
			} else {
				log.Printf("skipping multiquery for stream=%s, cid=%s", streamCid.Id, streamCid.Cid)
			}
		}
	}
	mqBody, err := json.Marshal(mq)
	if err != nil {
		log.Printf("error creating multiquery json: %v", err)
		return -1, err
	}

	req, err := http.NewRequestWithContext(mqCtx, "POST", CeramicUrl+"/api/v0/multiqueries", bytes.NewBuffer(mqBody))
	if err != nil {
		log.Printf("error creating multiquery request: %v", err)
		return -1, err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("error submitting multiquery: %v", err)
		return -1, err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("error reading multiquery response: %v", err)
		return -1, err
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("error in multiquery: %v", resp.StatusCode)
		return -1, errors.New("error in multiquery")
	}
	mqResp := make(map[string]cas.StreamState)
	if err = json.Unmarshal(respBody, &mqResp); err != nil {
		log.Printf("error unmarshaling multiquery response: %v", err)
		return -1, err
	}
	log.Printf("mq response: streams=%d", len(mqResp))
	if len(mqResp) > 0 {
		loaded := true
		for streamId, streamState := range mqResp {
			StreamCtr++
			log.Printf("loaded %d streams, id=%s", StreamCtr, streamId)
			// Only write CIDs beyond the newest CID already available for a stream (query by id-pos-index). This
			// reduces the number of DB writes, but not the number of Ceramic multi-queries.
			if latestStreamCid, err := db.GetLatestCid(streamId); err != nil {
				log.Printf("error writing stream/commit to db: %v", err)
			} else {
				idx := 0
				if latestStreamCid != nil {
					idx = *latestStreamCid.Position
				}
				for ; idx < len(streamState.Log); idx++ {
					commitState := streamState.Log[idx]
					streamCid := cas.StreamCid{
						Id:         streamId,
						Cid:        commitState.Cid,
						Loaded:     &loaded,
						CommitType: &commitState.Type,
						Position:   &idx,
					}
					if streamState.Metadata.Family != nil {
						streamCid.Family = streamState.Metadata.Family
					}
					if idx == 0 {
						streamCid.StreamType = &streamState.Type
						streamCid.Controller = &streamState.Metadata.Controllers[0]
					}
					if err = db.UpdateCid(&streamCid); err != nil {
						log.Printf("error writing stream/commit to db: %v", err)
					}
					CommitCtr++
					// Sleep for 1 second every 5 writes so we don't bust our throughput. TODO: Consider using another batch executor here.
					if (CommitCtr % 5) == 0 {
						time.Sleep(time.Second)
					}
					log.Printf("loaded %d commits, id=%s", CommitCtr, commitState.Cid)
				}
			}
		}
	}
	return 0, nil
}
