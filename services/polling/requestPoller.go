package polling

import (
	"context"
	"log"
	"time"

	"github.com/smrz2001/go-cas/models"
)

type RequestPoller struct {
	anchorDb     models.AnchorRepository
	stateDb      models.StateRepository
	pinPublisher models.QueuePublisher
}

func NewRequestPoller(anchorDb models.AnchorRepository, stateDb models.StateRepository, pinPublisher models.QueuePublisher) *RequestPoller {
	return &RequestPoller{
		anchorDb:     anchorDb,
		stateDb:      stateDb,
		pinPublisher: pinPublisher,
	}
}

func (rp RequestPoller) Run() {
	// Start from the last checkpoint
	prevCheckpoint, err := rp.stateDb.GetCheckpoint(models.CheckpointType_RequestPoll)
	if err != nil {
		log.Fatalf("requestpoll: error querying checkpoint: %v", err)
	}
	log.Printf("requestpoll: start checkpoint: %s", prevCheckpoint)
	newerThan := prevCheckpoint

	for {
		if anchorReqs, err := rp.anchorDb.GetRequests(
			models.RequestStatus_Pending,
			newerThan,
			time.Now().UTC(),
			nil,
			models.DbLoadLimit,
		); err != nil {
			log.Printf("requestpoll: error loading requests: %v", err)
		} else if len(anchorReqs) > 0 {
			log.Printf("requestpoll: found %d requests", len(anchorReqs))
			if nextCheckpoint, err := rp.sendRequestMessages(anchorReqs); err != nil {
				log.Printf("requestpoll: error processing requests: %v", err)
			} else
			// It's possible the checkpoint was updated even if a particular request in the batch failed to be queued
			if nextCheckpoint.After(newerThan) {
				log.Printf("requestpoll: old=%s, new=%s", newerThan, nextCheckpoint)
				if _, err = rp.stateDb.UpdateCheckpoint(models.CheckpointType_RequestPoll, nextCheckpoint); err != nil {
					log.Printf("requestpoll: error updating checkpoint %s: %v", nextCheckpoint, err)
				} else {
					// Only update checkpoint in-memory once it's been written to DB. This means that it's possible that
					// we might reprocess Anchor DB entries, but we can handle this.
					newerThan = nextCheckpoint
				}
			}
		}
		// Sleep even if we had errors so that we don't get stuck in a tight loop
		time.Sleep(models.DefaultTick)
	}
}

func (rp RequestPoller) sendRequestMessages(anchorReqs []*models.AnchorRequestMessage) (time.Time, error) {
	for _, anchorReq := range anchorReqs {
		req := anchorReq
		go func() {
			if _, err := rp.pinPublisher.SendMessage(context.Background(), req); err != nil {
				log.Printf("requestpoll: failed to send message: %v, %v", req, err)
				// If there's an error, ignore it. Pinning is an optimization and not absolutely necessary, so it's ok
				// if it's skipped for a few streams now and then. These streams will get loaded again eventually once
				// the request is processed by the loading service (if necessary).
			}
		}()
	}
	// Just return the timestamp from the last request in the list so that processing keeps moving along
	return anchorReqs[len(anchorReqs)-1].UpdatedAt, nil
}
