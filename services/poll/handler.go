package poll

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/abevier/go-sqs/gosqs"

	"github.com/smrz2001/go-cas/aws"
	"github.com/smrz2001/go-cas/db"
	"github.com/smrz2001/go-cas/models"
)

const PollMaxProcessingTime = 3 * time.Minute

type PollingService struct {
	anchorDb     *db.AnchorDatabase
	stateDb      *db.StateDatabase
	reqPublisher *gosqs.SQSPublisher
}

func NewPollingService() *PollingService {
	cfg, err := cas.AwsConfig()
	if err != nil {
		log.Fatalf("newCeramicLoader: error creating aws cfg: %v", err)
	}
	adbOpts := db.AnchorDbOpts{
		Host:     os.Getenv("PG_HOST"),
		Port:     os.Getenv("PG_PORT"),
		User:     os.Getenv("PG_USER"),
		Password: os.Getenv("PG_PASSWORD"),
		Name:     os.Getenv("PG_DB"),
	}
	client := sqs.NewFromConfig(cfg)
	return &PollingService{
		anchorDb: db.NewAnchorDb(adbOpts),
		stateDb:  db.NewStateDb(),
		reqPublisher: gosqs.NewPublisher(
			client,
			cas.GetQueueUrl(client, models.QueueType_Request),
			models.DefaultBatchMaxLinger,
		),
	}
}

func (p PollingService) Poll() {
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
			nextCheckpoint, err := p.sendEvents(anchorReqs)
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

func (p PollingService) sendEvents(anchorReqs []*models.AnchorRequestEvent) (time.Time, error) {
	// TODO: Use better context
	ctx, cancel := context.WithTimeout(context.Background(), PollMaxProcessingTime)
	defer cancel()

	checkpoint := time.Time{}
	for idx, anchorReq := range anchorReqs {
		if _, err := cas.PublishEvent(ctx, p.reqPublisher, anchorReq); err != nil {
			log.Printf("sendRequest: failed to queue event: %v, %v", anchorReqs[idx], err)
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
