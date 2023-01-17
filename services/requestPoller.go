package services

import (
	"context"
	"log"
	"time"

	"github.com/smrz2001/go-cas/models"
)

const PollMaxProcessingTime = 3 * time.Minute

type PollingService struct {
	anchorDb     anchorRepository
	stateDb      stateRepository
	reqPublisher queuePublisher
}

func NewPollingService(anchorDb anchorRepository, stateDb stateRepository, reqPublisher queuePublisher) *PollingService {
	return &PollingService{
		anchorDb:     anchorDb,
		stateDb:      stateDb,
		reqPublisher: reqPublisher,
	}
}

func (p PollingService) Run() {
	prevCheckpoint, err := p.stateDb.GetCheckpoint(models.CheckpointType_Poll)
	if err != nil {
		log.Fatalf("poll: error querying checkpoint: %v", err)
	}
	log.Printf("poll: db checkpoint: %s", prevCheckpoint)
	checkpoint := prevCheckpoint

	for {
		if anchorReqs, err := p.anchorDb.RequestsSinceCheckpoint(checkpoint, models.DbLoadLimit); err != nil {
			log.Printf("poll: error loading requests: %v", err)
		} else if len(anchorReqs) > 0 {
			log.Printf("poll: found %d requests", len(anchorReqs))
			if nextCheckpoint, err := p.sendRequestMessages(anchorReqs); err != nil {
				log.Printf("poll: error processing requests: %v", err)
			} else
			// It's possible the checkpoint was updated even if a particular request in the batch failed to be queued
			if nextCheckpoint.After(checkpoint) {
				log.Printf("poll: old=%s, new=%s", checkpoint, nextCheckpoint)
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

func (p PollingService) sendRequestMessages(anchorReqs []*models.AnchorRequestMessage) (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), PollMaxProcessingTime)
	defer cancel()

	checkpoint := time.Time{}
	for idx, anchorReq := range anchorReqs {
		log.Printf("poll: %v", anchorReqs[idx])
		if _, err := p.reqPublisher.SendMessage(ctx, anchorReq); err != nil {
			log.Printf("poll: failed to send message: %v, %v", anchorReqs[idx], err)
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
