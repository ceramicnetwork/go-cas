package poller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/abevier/tsk/futures"
	"github.com/abevier/tsk/ratelimiter"
	"github.com/smrz2001/go-cas"
	"github.com/smrz2001/go-cas/db"
	"github.com/smrz2001/go-cas/models"
	"github.com/smrz2001/go-cas/queue/messages"
)

type Migration struct {
	anchorDb    *db.AnchorDatabase
	stateDb     *db.StateDatabase
	rateLimiter *ratelimiter.RateLimiter[*messages.AnchorRequest, *casResponse]
	url         string
}

type casResponse struct {
}

func NewMigration() *Migration {
	cfg, err := cas.AwsConfig()
	if err != nil {
		log.Fatalf("failed to create aws cfg: %v", err)
	}
	m := Migration{
		anchorDb: db.NewAnchorDb(),
		stateDb:  db.NewStateDb(cfg),
		url:      os.Getenv("CAS_URL"),
	}
	rlOpts := ratelimiter.RateLimiterOpts{
		Limit:             models.DefaultRateLimit,
		Burst:             models.DefaultRateLimit,
		FullQueueStrategy: ratelimiter.BlockWhenFull,
	}
	m.rateLimiter = ratelimiter.New[*messages.AnchorRequest, *casResponse](rlOpts, m.casRequest)
	return &m
}

// TODO: Initialize with server context and use that for rate limiter
func (m Migration) Migrate() {
	prevCheckpoint, err := m.stateDb.GetCheckpoint(models.CheckpointType_Migration)
	if err != nil {
		log.Fatalf("poll: error querying checkpoint: %v", err)
	}
	log.Printf("poll: db checkpoint: %s", prevCheckpoint)
	checkpoint := prevCheckpoint

	for {
		if anchorReqs, err := m.anchorDb.Poll(checkpoint, models.DbLoadLimit); err != nil {
			log.Printf("migrate: error loading requests: %v", err)
		} else if len(anchorReqs) > 0 {
			nextCheckpoint, err := m.migrateRequests(context.Background(), anchorReqs)
			if err != nil {
				log.Printf("migrate: error processing requests: %v", err)
			}
			log.Printf("migrate: old=%s, new=%s", checkpoint, nextCheckpoint)

			// It's possible the checkpoint was updated even if a particular request in the batch failed to be processed
			if nextCheckpoint.After(checkpoint) {
				if _, err = m.stateDb.UpdateCheckpoint(models.CheckpointType_Poll, nextCheckpoint); err != nil {
					log.Printf("poll: error updating checkpoint %s: %v", nextCheckpoint, err)
				} else {
					// Only update checkpoint in-memory once it's been written to DB. This means that it's possible that
					// we might reprocess Anchor DB entries, but that's ok.
					checkpoint = nextCheckpoint
				}
			}
		}
		// Sleep even if we had errors so that we don't get stuck in a tight loop
		time.Sleep(models.DefaultTick)
	}
}

// TODOs:
// - Add separate DynamoDB table to store requests using CAS API responses
// - Poll for anchoring results from new Prod CAS DB
// - Clear out entries as they get resolved
// - Move on to the next batch once all entries are resolved
// - Panic if it takes longer than 12 hours for a batch to clear
// - Store checkpoints so we always know where we are in this process and can stop/resume at any time
func (m Migration) migrateRequests(ctx context.Context, anchorReqs []*messages.AnchorRequest) (time.Time, error) {
	reqFutures := make([]*futures.Future[*casResponse], len(anchorReqs))
	for idx, anchorReq := range anchorReqs {
		reqFutures[idx] = m.rateLimiter.SubmitF(ctx, anchorReq)
	}
	checkpoint := time.Time{}
	for idx, reqFuture := range reqFutures {
		if _, err := reqFuture.Get(ctx); err != nil {
			log.Printf("migrateRequests: failed to process request: %v, %v", anchorReqs[idx], err)
			// If there's an error, return so that this entry is reprocessed. This is unlikely to happen but if a tiny
			// number of duplicates makes it through, that's ok. It's better to anchor some requests more than once than
			// to not anchor some at all.
			return time.Time{}, err
		}
		checkpoint = anchorReqs[idx].CreatedAt
	}
	return checkpoint, nil
}

func (m Migration) casRequest(ctx context.Context, anchorReq *messages.AnchorRequest) (*casResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, models.DefaultHttpWaitTime)
	defer cancel()

	log.Printf("casRequest: request=%+v", anchorReq)
	type anchorRequest struct {
		DocId    string `json:"docId"`
		StreamId string `json:"streamId"`
		Cid      string `json:"cid"`
	}
	reqBody, err := json.Marshal(anchorRequest{
		anchorReq.StreamId,
		anchorReq.StreamId,
		anchorReq.Cid,
	})
	if err != nil {
		log.Printf("casRequest: error creating anchor request json: %v", err)
		// Return the error in the task queue submission result
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", m.url+"/api/v0/requests", bytes.NewBuffer(reqBody))
	if err != nil {
		log.Printf("casRequest: error creating anchor request: %v", err)
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("casRequest: error submitting anchor request: %v", err)
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("casRequest: error reading response: %v", err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("casRequest: error in response: %v, %s", resp.StatusCode, respBody)
		return nil, fmt.Errorf("casRequest: error in response: %v, %s", resp.StatusCode, respBody)
	}
	log.Printf("casRequest: response=%s", respBody)
	return nil, nil
}
