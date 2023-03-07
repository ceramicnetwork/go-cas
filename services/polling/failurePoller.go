package polling

import (
	"context"
	"log"
	"time"

	"github.com/ceramicnetwork/go-cas/models"
)

type FailurePoller struct {
	anchorDb      models.AnchorRepository
	stateDb       models.StateRepository
	loadPublisher models.QueuePublisher
}

func NewFailurePoller(anchorDb models.AnchorRepository, stateDb models.StateRepository, reqPublisher models.QueuePublisher) *FailurePoller {
	return &FailurePoller{
		anchorDb,
		stateDb,
		reqPublisher,
	}
}

func (fp FailurePoller) Run() {
	// Start from the last checkpoint or 2 days ago, whichever is sooner.
	prevCheckpoint, err := fp.stateDb.GetCheckpoint(models.CheckpointType_FailurePoll)
	if err != nil {
		log.Fatalf("failurepoll: error querying checkpoint: %v", err)
	}
	log.Printf("failurepoll: start checkpoint: %s", prevCheckpoint)
	newerThan := time.Now().UTC().Add(-48 * time.Hour)
	if prevCheckpoint.After(newerThan) {
		newerThan = prevCheckpoint
	}

	for {
		if anchorReqs, err := fp.anchorDb.GetRequests(
			models.RequestStatus_Failed,
			newerThan,
			time.Now().UTC().Add(-6*time.Hour),
			[]string{"permanently", "conflict"},
			models.DbLoadLimit,
		); err != nil {
			log.Printf("failurepoll: error loading requests: %v", err)
		} else if len(anchorReqs) > 0 {
			log.Printf("failurepoll: found %d requests", len(anchorReqs))
			if nextCheckpoint, err := fp.sendFailedRequestMessages(anchorReqs); err != nil {
				log.Printf("failurepoll: error processing requests: %v", err)
			} else
			// It's possible the checkpoint was updated even if a particular request in the batch failed to be queued
			if nextCheckpoint.After(newerThan) {
				log.Printf("failurepoll: old=%s, new=%s", newerThan, nextCheckpoint)
				if _, err = fp.stateDb.UpdateCheckpoint(models.CheckpointType_FailurePoll, nextCheckpoint); err != nil {
					log.Printf("failurepoll: error updating checkpoint %s: %v", nextCheckpoint, err)
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

func (fp FailurePoller) sendFailedRequestMessages(anchorReqs []*models.AnchorRequestMessage) (time.Time, error) {
	for _, anchorReq := range anchorReqs {
		req := anchorReq
		go func() {
			if _, err := fp.loadPublisher.SendMessage(context.Background(), req); err != nil {
				log.Printf("failurepoll: failed to send message: %v, %v", req, err)
				// If there's an error, ignore it. The timestamp for this request will not be updated and it
				// will just be picked up in the next iteration.
			}
		}()
	}
	// Just return the timestamp from the last request in the list so that processing keeps moving along
	return anchorReqs[len(anchorReqs)-1].UpdatedAt, nil
}
