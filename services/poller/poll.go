package poller

import (
	"log"
	"time"

	"github.com/smrz2001/go-cas/aws"
	"github.com/smrz2001/go-cas/db"
	"github.com/smrz2001/go-cas/models"
	"github.com/smrz2001/go-cas/services/loader"
)

var RowCtr = 0

type RequestPoller struct {
	anchorDb *db.AnchorDatabase
	stateDb  *db.StateDatabase
}

func NewRequestPoller() *RequestPoller {
	anchorDb := db.NewAnchorDb()
	cfg, err := aws.Config()
	if err != nil {
		log.Fatalf("failed to create aws cfg: %v", err)
	}
	stateDb := db.NewStateDb(cfg)
	return &RequestPoller{anchorDb, stateDb}
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
			if nextCheckpoint, err := p.processRequests(anchorReqs); err != nil {
				log.Printf("poll: error processing requests: %v", err)
			} else if nextCheckpoint.After(checkpoint) {
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

func (p RequestPoller) processRequests(anchorReqs []*models.AnchorRequest) (time.Time, error) {
	checkpoint := time.Time{}
	for _, anchorReq := range anchorReqs {
		loader.RequestCh <- models.StreamCid{
			Id:  anchorReq.StreamId,
			Cid: anchorReq.Cid,
		}
		checkpoint = anchorReq.CreatedAt
		RowCtr++
		log.Printf("processed %d rows: %v", RowCtr, anchorReq)
	}
	return checkpoint, nil
}
