package services

import (
	"context"
	"fmt"
	"time"

	"github.com/ceramicnetwork/go-cas/models"
)

const pollTick = 5 * time.Minute
const dbLoadLimit = 1000

// Only look for unprocessed requests as far back as 2 days
const startCheckpointDelta = -48 * time.Hour

// Only look for unprocessed requests older than 12 hours
const endCheckpointDelta = -12 * time.Hour

type RequestPoller struct {
	anchorDb          models.AnchorRepository
	stateDb           models.StateRepository
	validatePublisher models.QueuePublisher
	logger            models.Logger
	notif             models.Notifier
	tick              time.Duration
}

func NewRequestPoller(
	logger models.Logger,
	anchorDb models.AnchorRepository,
	stateDb models.StateRepository,
	validatePublisher models.QueuePublisher,
	notif models.Notifier,
) *RequestPoller {
	return &RequestPoller{
		anchorDb:          anchorDb,
		stateDb:           stateDb,
		validatePublisher: validatePublisher,
		logger:            logger,
		notif:             notif,
		tick:              pollTick,
	}
}

func (p RequestPoller) Run(ctx context.Context) {
	// Start from the last checkpoint
	prevCheckpoint, err := p.stateDb.GetCheckpoint(ctx, models.CheckpointType_RequestPoll)
	if err != nil {
		p.logger.Fatalf("error querying checkpoint: %v", err)
	}

	startCheckpoint := time.Now().UTC().Add(startCheckpointDelta)
	if prevCheckpoint.After(startCheckpoint) {
		startCheckpoint = prevCheckpoint
	}
	p.logger.Infof("start checkpoint: %s", prevCheckpoint)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			endCheckpoint := time.Now().UTC().Add(endCheckpointDelta)
			anchorReqs, err := p.anchorDb.GetRequests(
				ctx,
				models.RequestStatus_Pending,
				startCheckpoint,
				endCheckpoint,
				dbLoadLimit,
			)
			if err != nil {
				p.logger.Errorf("error loading requests: %v", err)
			} else if len(anchorReqs) > 0 {
				p.logger.Debugf("found %d requests newer than %s", len(anchorReqs), startCheckpoint)
				// Send an alert because we shouldn't have found any old unprocessed requests
				err = p.notif.SendAlert(
					models.AlertTitle,
					models.AlertDesc_Unprocessed,
					fmt.Sprintf(models.AlertFmt_Unprocessed, len(anchorReqs), startCheckpoint, endCheckpoint),
				)
				if err != nil {
					p.logger.Errorf("error sending alert: %v", err)
				}

				anchorReqMsgs := make([]*models.AnchorRequestMessage, len(anchorReqs))
				for idx, anchorReq := range anchorReqs {
					anchorReqMsgs[idx] = &models.AnchorRequestMessage{
						Id:        anchorReq.Id,
						StreamId:  anchorReq.StreamId,
						Cid:       anchorReq.Cid,
						Origin:    anchorReq.Origin,
						Timestamp: anchorReq.Timestamp,
						CreatedAt: anchorReq.CreatedAt,
					}
				}
				// It's possible the checkpoint was updated even if a particular request in the batch failed to be
				// queued.
				if nextCheckpoint := p.sendRequestMessages(ctx, anchorReqMsgs); nextCheckpoint.After(startCheckpoint) {
					p.logger.Debugf("checkpoints: start=%s, next=%s", startCheckpoint, nextCheckpoint)
					if _, err = p.stateDb.UpdateCheckpoint(ctx, models.CheckpointType_RequestPoll, nextCheckpoint); err != nil {
						p.logger.Errorf("error updating checkpoint %s: %v", nextCheckpoint, err)
					} else {
						// Only update checkpoint in-memory once it's been written to DB. This means that it's possible
						// that we might reprocess Anchor DB entries, but we can handle this.
						startCheckpoint = nextCheckpoint
					}
				}
			}
			// Sleep even if we had errors so that we don't get stuck in a tight loop
			time.Sleep(p.tick)
		}
	}
}

func (p RequestPoller) sendRequestMessages(ctx context.Context, anchorReqs []*models.AnchorRequestMessage) time.Time {
	processedCheckpoint := anchorReqs[0].CreatedAt.Add(-time.Millisecond)
	for _, anchorReq := range anchorReqs {
		if _, err := p.validatePublisher.SendMessage(ctx, anchorReq); err != nil {
			p.logger.Errorf("error sending message: %v, %v", anchorReq, err)
			break
		}
		processedCheckpoint = anchorReq.CreatedAt
	}
	return processedCheckpoint
}
