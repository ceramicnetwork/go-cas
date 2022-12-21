package poller

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/abevier/tsk/futures"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/smrz2001/go-cas/db"
	"github.com/smrz2001/go-cas/models"
	"github.com/smrz2001/go-cas/queue"
	"github.com/smrz2001/go-cas/queue/messages"
)

const PollMaxProcessingTime = 3 * time.Minute

type RequestPoller struct {
	anchorDb *db.AnchorDatabase
	stateDb  *db.StateDatabase
	requestQ *queue.Queue[*messages.AnchorRequest]
}

func NewRequestPoller(cfg aws.Config) *RequestPoller {
	adbOpts := db.AnchorDbOpts{
		Host:     os.Getenv("PG_HOST"),
		Port:     os.Getenv("PG_PORT"),
		User:     os.Getenv("PG_USER"),
		Password: os.Getenv("PG_PASSWORD"),
		Name:     os.Getenv("PG_DB"),
	}
	return &RequestPoller{
		anchorDb: db.NewAnchorDb(adbOpts),
		stateDb:  db.NewStateDb(cfg),
		requestQ: queue.NewQueue[*messages.AnchorRequest](cfg, string(queue.QueueType_Request)),
	}
}

func (p RequestPoller) Poll() {
	prevCheckpoint, err := p.stateDb.GetCheckpoint(models.CheckpointType_Poll)
	if err != nil {
		log.Fatalf("poll: error querying checkpoint: %v", err)
	}
	log.Printf("poll: db checkpoint: %s", prevCheckpoint)
	checkpoint := prevCheckpoint

	for {
		if anchorReqs, err := p.anchorDb.PollRequests(checkpoint, models.DbLoadLimit); err != nil {
			log.Printf("poll: error loading requests: %v", err)
		} else if len(anchorReqs) > 0 {
			nextCheckpoint, err := p.enqueueRequests(anchorReqs)
			if err != nil {
				log.Printf("poll: error processing requests: %v", err)
			}
			log.Printf("poll: old=%s, new=%s", checkpoint, nextCheckpoint)

			// It's possible the checkpoint was updated even if a particular request in the batch failed to be queued
			if nextCheckpoint.After(checkpoint) {
				if _, err = p.stateDb.UpdateCheckpoint(models.CheckpointType_Poll, nextCheckpoint); err != nil {
					log.Printf("poll: error updating checkpoint %s: %v", nextCheckpoint, err)
				} else {
					// Only update checkpoint in-memory once it's been written to DB. This means that it's possible that
					// we might reprocess Anchor DB entries, but we can handle this.
					checkpoint = nextCheckpoint
				}
			}
		}
		// Sleep even if we had errors so that we don't get stuck in a tight loop
		time.Sleep(models.DefaultTick)
	}
}

func (p RequestPoller) enqueueRequests(anchorReqs []*messages.AnchorRequest) (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), PollMaxProcessingTime)
	defer cancel()

	qfs := make([]*futures.Future[string], len(anchorReqs))
	for idx, anchorReq := range anchorReqs {
		qfs[idx] = p.requestQ.EnqueueF(anchorReq)
	}
	checkpoint := time.Time{}
	for idx, qf := range qfs {
		// TODO: Retry with exponential backoff. Would be nice to make sure a particular batch gets enqueued because
		// otherwise we could have a bunch of duplicated messages if one future fails while others down the line have
		// passed.
		if _, err := qf.Get(ctx); err != nil {
			log.Printf("enqueueRequests: failed to process request: %v, %v", anchorReqs[idx], err)
			// If there's an error, return so that this entry is reprocessed. SQS deduplication will likely take care of
			// any duplicate messages, though if there is a tiny number of duplicates that makes it through, that's ok.
			// It's better to anchor some requests more than once than to not anchor some at all.
			return checkpoint, err
		}
		// Updated the checkpoint to the last entry we were able to enqueue successfully
		checkpoint = anchorReqs[idx].CreatedAt
	}
	return checkpoint, nil
}
