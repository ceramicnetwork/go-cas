package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/abevier/tsk/futures"
	"github.com/abevier/tsk/ratelimiter"

	"github.com/smrz2001/go-cas"
	"github.com/smrz2001/go-cas/db"
	"github.com/smrz2001/go-cas/models"
	"github.com/smrz2001/go-cas/queue/messages"
)

const defaultMaxAnchorInterval = 12 * time.Hour
const defaultBackoffInterval = 2 * time.Second

type Migration struct {
	oldAnchorDb    *db.AnchorDatabase
	newAnchorDb    *db.AnchorDatabase
	stateDb        *db.StateDatabase
	migrationDb    *MigrationDatabase
	rateLimiter    *ratelimiter.RateLimiter[*messages.AnchorRequest, *casResponse]
	casUrl         string
	anchorInterval time.Duration
	threeIdStreams []string
}

type casRequest struct {
	DocId    string `json:"docId"`
	StreamId string `json:"streamId"`
	Cid      string `json:"cid"`
}

// We only care about the returned UUID so that we can poll for its resolution in the anchor DB within the maximum
// anchor interval.
type casResponse struct {
	Id        string `json:"id"`
	Status    string `json:"-"`
	Cid       string `json:"-"`
	DocId     string `json:"-"`
	StreamId  string `json:"-"`
	Message   string `json:"-"`
	CreatedAt int64  `json:"-"`
	UpdatedAt int64  `json:"-"`
}

func NewMigration() *Migration {
	cfg, err := cas.AwsConfig()
	if err != nil {
		log.Fatalf("migration: failed to create aws cfg: %v", err)
	}
	anchorInterval := defaultMaxAnchorInterval
	if anchorIntervalEnv, found := os.LookupEnv(os.Getenv("CAS_ANCHOR_INTERVAL")); found {
		if parsedAnchorInterval, err := time.ParseDuration(anchorIntervalEnv); err == nil {
			anchorInterval = parsedAnchorInterval
		}
	}
	oldAdbOpts := db.AnchorDbOpts{
		Host:     os.Getenv("PG_HOST"),
		Port:     os.Getenv("PG_PORT"),
		User:     os.Getenv("PG_USER"),
		Password: os.Getenv("PG_PASSWORD"),
		Name:     os.Getenv("PG_DB"),
	}
	newAdbOpts := db.AnchorDbOpts{
		Host:     os.Getenv("NEW_PG_HOST"),
		Port:     os.Getenv("NEW_PG_PORT"),
		User:     os.Getenv("NEW_PG_USER"),
		Password: os.Getenv("NEW_PG_PASSWORD"),
		Name:     os.Getenv("NEW_PG_DB"),
	}
	m := Migration{
		oldAnchorDb:    db.NewAnchorDb(oldAdbOpts),
		newAnchorDb:    db.NewAnchorDb(newAdbOpts),
		stateDb:        db.NewStateDb(cfg),
		migrationDb:    NewMigrationDb(cfg),
		casUrl:         os.Getenv("CAS_URL"),
		anchorInterval: anchorInterval,
	}
	rlOpts := ratelimiter.RateLimiterOpts{
		Limit:             models.DefaultRateLimit,
		Burst:             models.DefaultRateLimit,
		FullQueueStrategy: ratelimiter.BlockWhenFull,
	}
	m.rateLimiter = ratelimiter.New[*messages.AnchorRequest, *casResponse](rlOpts, m.casRequest)
	m.threeIdStreams = strings.Split(os.Getenv("3ID_STREAMS"), ",")
	sort.Strings(m.threeIdStreams)
	return &m
}

// Migrate pulls requests from the old anchor DB, makes CAS API requests for them, then monitors them for some terminal
// anchor status. Each of the steps in this flow are blocking and will fail the entire run if there is an error.
func (m Migration) Migrate() {
	for {
		// 1. Fetch the start checkpoint for the current batch.
		startCheckpoint, err := m.stateDb.GetCheckpoint(models.CheckpointType_MigrationStart)
		if err != nil {
			log.Fatalf("migrate: error querying start checkpoint: %v", err)
		} else
		// 2. Check whether the current batch being migrated has completed. If it's been more than the maximum anchor
		//    interval since the last posted CAS request for the batch, fail the run.
		if err = m.checkRequests(startCheckpoint); err != nil {
			log.Fatalf("migrate: error verifying batch: %v", err)
		} else
		// 3. Copy the end checkpoint into the start checkpoint.
		if endCheckpoint, err := m.stateDb.GetCheckpoint(models.CheckpointType_MigrationEnd); err != nil {
			log.Fatalf("migrate: error querying end checkpoint: %v", err)
		} else if _, err = m.stateDb.UpdateCheckpoint(models.CheckpointType_MigrationStart, endCheckpoint); err != nil {
			log.Fatalf("migrate: error updating start checkpoint %s: %v", endCheckpoint, err)
		} else
		// 4. Get the next batch of requests from the anchor DB.
		if endCheckpoint, err = m.migrateRequests(context.Background(), endCheckpoint); err != nil {
			log.Fatalf("migrateRequests: error loading requests: %v", err)
		} else
		// 5. If the checkpoint remains the same, we're done!
		if endCheckpoint == startCheckpoint {
			log.Println("Woohoo! We're done! 🥳")
			return
		} else
		// 6. Set the end checkpoint based on the new batch of requests.
		if _, err = m.stateDb.UpdateCheckpoint(models.CheckpointType_MigrationEnd, endCheckpoint); err != nil {
			log.Fatalf("migrate: error updating end checkpoint %s: %v", endCheckpoint, err)
		}
		// Sleep even if we had errors so that we don't get stuck in a tight loop
		time.Sleep(models.DefaultTick)
	}
}

// checkRequests will look for *any* entries being present in the migration DB. This means that their anchor status
// hasn't been resolved yet. If it's been more than the maximum anchoring interval, we'll fail this run.
func (m Migration) checkRequests(checkpoint time.Time) error {
	processFn := func(request batchRequest) (bool, error) {
		// Check status of request in the new anchor DB before checking for the maximum anchor interval. This makes
		// it possible to run this code after an arbitrary amount of time and not fail, assuming all requests were
		// actually anchored.
		if resolved, err := m.checkRequestStatus(checkpoint, request.RequestId); err != nil {
			return false, err
		} else if !resolved {
			// Check if an unresolved entry has been waiting for more than the maximum anchor interval
			if time.Now().Add(-m.anchorInterval).After(request.Ts) {
				return false, fmt.Errorf("checkRequests: batch not anchored in time=%v", m.anchorInterval)
			}
		}
		// Keep iterating till the end of the batch
		return true, nil
	}
	// This loop will block until the batch is resolved, one way or another - either all requests have been resolved in
	// the new anchor DB, or the maximum anchor interval has passed.
	for {
		if foundItems, err := m.migrationDb.IterateBatch(checkpoint, false, processFn); err != nil {
			return err
		} else if !foundItems {
			// If no requests were found, exit.
			return nil
		}
		// Sleep even if we had errors so that we don't get stuck in a tight loop
		time.Sleep(models.DefaultTick)
	}
}

// migrateRequests makes new requests for each request in the batch picked up from the old anchor DB
func (m Migration) migrateRequests(ctx context.Context, checkpoint time.Time) (time.Time, error) {
	anchorReqs, err := m.oldAnchorDb.PollRequests(checkpoint, models.DbLoadLimit)
	if err != nil {
		return time.Time{}, err
	} else if len(anchorReqs) == 0 {
		// Nothing we need to do and no change in the checkpoint
		return checkpoint, nil
	}
	reqFutures := make([]*futures.Future[*casResponse], len(anchorReqs))
	newCheckpoint := checkpoint
	for idx, anchorReq := range anchorReqs {
		// Exclude 3ID streams
		if sort.SearchStrings(m.threeIdStreams, anchorReq.StreamId) == len(m.threeIdStreams) {
			reqFutures[idx] = m.rateLimiter.SubmitF(ctx, anchorReq)
			newCheckpoint = anchorReq.CreatedAt
		}
	}
	var casResp *casResponse
	// This loop will block until all requests to the CAS API have been made, and corresponding entries created in the
	// migration DB.
	for idx, reqFuture := range reqFutures {
		// Loop with constant backoff till the CAS API request succeeds (ignore the error)
		backoff.Retry(
			func() error {
				if casResp, err = reqFuture.Get(ctx); err != nil {
					log.Printf("migrateRequests: error processing request: %v, %v", anchorReqs[idx], err)
					return err
				}
				return nil
			},
			backoff.NewConstantBackOff(defaultBackoffInterval),
		)
		// Loop with constant backoff till the DB write succeeds (ignore the error)
		backoff.Retry(
			func() error {
				// TODO: Hack to bypass 400 errors
				if casResp != nil {
					return m.migrationDb.WriteRequest(checkpoint, casResp.Id)
				}
				return nil
			},
			backoff.NewConstantBackOff(defaultBackoffInterval),
		)
	}
	return newCheckpoint, nil
}

func (m Migration) checkRequestStatus(checkpoint time.Time, requestId string) (bool, error) {
	if anchorRequests, err := m.newAnchorDb.Query("SELECT * FROM request WHERE id = $1", requestId); err != nil {
		return false, err
	} else if len(anchorRequests) == 0 {
		return false, fmt.Errorf("checkRequestStatus: error fetching request=%s", requestId)
	} else if (anchorRequests[0].Status == db.RequestStatus_Completed) || (anchorRequests[0].Status == db.RequestStatus_Failed) {
		// Delete a resolved anchor request from the migration DB
		if err = m.migrationDb.DeleteRequest(checkpoint, requestId); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// casRequest makes the actual CAS API request and returns an unmarshalled response
func (m Migration) casRequest(ctx context.Context, anchorReq *messages.AnchorRequest) (*casResponse, error) {
	rCtx, rCancel := context.WithTimeout(ctx, models.DefaultHttpWaitTime)
	defer rCancel()

	log.Printf("casRequest: request=%+v", anchorReq)
	reqBody, err := json.Marshal(casRequest{
		anchorReq.StreamId,
		anchorReq.StreamId,
		anchorReq.Cid,
	})
	if err != nil {
		log.Printf("casRequest: error creating anchor request json: %v", err)
		// Return the error in the task queue submission result
		return nil, err
	}
	req, err := http.NewRequestWithContext(rCtx, "POST", m.casUrl+"/api/v0/requests", bytes.NewBuffer(reqBody))
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
	// TODO: Figure out what to do for these errors
	// 400, {"error":"Creating request with streamId kjzl6cwe1jw149k0gnex0czr1i37yfiqsenpzl1qbd6x3es00mlzadnc0bqxtzl and
	// commit CID bagcqceravpdwbjf44fsx2xxrxwny6qb4zxnptkwjk5wt3hw5oynvvggnpqiq failed: Cannot read properties of
	// undefined (reading 'cid')"}
	if resp.StatusCode == http.StatusBadRequest {
		log.Printf("casRequest: error in response: %v, %s", resp.StatusCode, respBody)
		return nil, nil
	}
	if (resp.StatusCode != http.StatusCreated) && (resp.StatusCode != http.StatusAccepted) {
		log.Printf("casRequest: error in response: %v, %s", resp.StatusCode, respBody)
		return nil, fmt.Errorf("casRequest: error in response: %v, %s", resp.StatusCode, respBody)
	}
	log.Printf("casRequest: response=%s", respBody)
	casResp := &casResponse{}
	if err = json.Unmarshal(respBody, casResp); err != nil {
		log.Printf("casRequest: error unmarshaling response: %v", err)
	}
	return casResp, nil
}
