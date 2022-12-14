package poller

import (
	"log"
	"time"

	"github.com/smrz2001/go-cas"
	"github.com/smrz2001/go-cas/db"
	"github.com/smrz2001/go-cas/models"
	"github.com/smrz2001/go-cas/queue"
	"github.com/smrz2001/go-cas/queue/messages"
)

type RequestPoller struct {
	anchorDb *db.AnchorDatabase
	stateDb  *db.StateDatabase
	requestQ *queue.Queue[*messages.AnchorRequest]
}

func NewRequestPoller() *RequestPoller {
	anchorDb := db.NewAnchorDb()
	cfg, err := cas.AwsConfig()
	if err != nil {
		log.Fatalf("failed to create aws cfg: %v", err)
	}
	return &RequestPoller{
		anchorDb,
		db.NewStateDb(cfg),
		queue.NewQueue[*messages.AnchorRequest](cfg, string(queue.QueueType_Request)),
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
		if anchorReqs, err := p.anchorDb.Poll(checkpoint, models.DbLoadLimit); err != nil {
			log.Printf("poll: error loading requests: %v", err)
		} else if len(anchorReqs) > 0 {
			nextCheckpoint, err := p.processRequests(anchorReqs)
			if err != nil {
				log.Printf("poll: error processing requests: %v", err)
			}
			// It's possible the checkpoint was updated even if a particular request in the batch failed to be queued to
			// SQS.
			if nextCheckpoint.After(checkpoint) {
				if _, err = p.stateDb.UpdateCheckpoint(models.CheckpointType_Poll, nextCheckpoint); err != nil {
					log.Printf("poll: error updating checkpoint: %v", err)
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

func (p RequestPoller) processRequests(anchorReqs []*messages.AnchorRequest) (time.Time, error) {
	checkpoint := time.Time{}
	for _, anchorReq := range anchorReqs {
		// TODO: Batch requests to SQS
		if _, err := p.requestQ.Enqueue(anchorReq); err != nil {
			log.Printf("processRequests: failed to queue request: %v, %v", anchorReq, err)
			// If there's an error, return so that this entry is reprocessed. SQS deduplication will likely take care of
			// any duplicate messages, though if there is a tiny number of duplicates that makes it through, that's ok.
			// It's better to anchor some requests more than once than to not anchor some at all.
			return time.Time{}, err
		}
		checkpoint = anchorReq.CreatedAt
	}
	return checkpoint, nil
}
