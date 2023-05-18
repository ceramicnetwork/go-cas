package services

import (
	"context"
	"log"
	"time"

	"github.com/ceramicnetwork/go-cas/models"
)

type RequestPoller struct {
	anchorDb       models.AnchorRepository
	stateDb        models.StateRepository
	readyPublisher models.QueuePublisher
}

func NewRequestPoller(anchorDb models.AnchorRepository, stateDb models.StateRepository, readyPublisher models.QueuePublisher) *RequestPoller {
	return &RequestPoller{
		anchorDb:       anchorDb,
		stateDb:        stateDb,
		readyPublisher: readyPublisher,
	}
}

func (rp RequestPoller) Run(ctx context.Context) {
	// Start from the last checkpoint
	prevCheckpoint, err := rp.stateDb.GetCheckpoint(models.CheckpointType_RequestPoll)
	if err != nil {
		log.Fatalf("requestpoll: error querying checkpoint: %v", err)
	}
	log.Printf("requestpoll: start checkpoint: %s", prevCheckpoint)
	since := prevCheckpoint

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if anchorReqs, err := rp.anchorDb.GetRequests(
				models.RequestStatus_Pending,
				since,
				models.DbLoadLimit,
			); err != nil {
				log.Printf("requestpoll: error loading requests: %v", err)
			} else if len(anchorReqs) > 0 {
				log.Printf("requestpoll: found %d requests newer than %s", len(anchorReqs), since)
				// It's possible the checkpoint was updated even if a particular request in the batch failed to be queued
				if nextCheckpoint := rp.sendRequestMessages(ctx, anchorReqs); nextCheckpoint.After(since) {
					log.Printf("requestpoll: old=%s, new=%s", since, nextCheckpoint)
					if _, err = rp.stateDb.UpdateCheckpoint(models.CheckpointType_RequestPoll, nextCheckpoint); err != nil {
						log.Printf("requestpoll: error updating checkpoint %s: %v", nextCheckpoint, err)
					} else {
						// Only update checkpoint in-memory once it's been written to DB. This means that it's possible that
						// we might reprocess Anchor DB entries, but we can handle this.
						since = nextCheckpoint
					}
				}

			}
			// Sleep even if we had errors so that we don't get stuck in a tight loop
			time.Sleep(models.DefaultTick)
		}
	}
}

func (rp RequestPoller) sendRequestMessages(ctx context.Context, anchorReqs []*models.AnchorRequestMessage) time.Time {
	processedCheckpoint := anchorReqs[0].CreatedAt.Add(-time.Millisecond)
	for _, anchorReq := range anchorReqs {
		// Ideally, we send messages from inside a goroutine but that makes it difficult to ensure the correctness of
		// the checkpoint (e.g. what if one message from the middle of the batch fails to send?). For now, while we're
		// still using two databases, make this sequential. Once we have a single database, we won't need the poller at
		// all - the API can then write directly to DynamoDB/SQS.
		if _, err := rp.readyPublisher.SendMessage(ctx, anchorReq); err != nil {
			log.Printf("requestpoll: failed to send message: %v, %v", anchorReq, err)
			break
		}
		processedCheckpoint = anchorReq.CreatedAt
	}
	return processedCheckpoint
}
