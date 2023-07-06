package services

import (
	"context"
	"encoding/json"
	"log"

	"github.com/google/uuid"

	"github.com/ceramicnetwork/go-cas/models"
)

type ValidationService struct {
	stateDb         models.StateRepository
	readyPublisher  models.QueuePublisher
	statusPublisher models.QueuePublisher
	metricService   models.MetricService
}

func NewValidationService(stateDb models.StateRepository, readyPublisher models.QueuePublisher, statusPublisher models.QueuePublisher, metricService models.MetricService) *ValidationService {
	return &ValidationService{stateDb, readyPublisher, statusPublisher, metricService}
}

func (v ValidationService) Validate(ctx context.Context, msgBody string) error {
	anchorReq := new(models.AnchorRequestMessage)
	if err := json.Unmarshal([]byte(msgBody), anchorReq); err != nil {
		return err
	}
	streamCid := &models.StreamCid{
		StreamId:  anchorReq.StreamId,
		Cid:       anchorReq.Cid,
		CreatedAt: anchorReq.CreatedAt,
	}
	newTip := &models.StreamTip{
		StreamId:  anchorReq.StreamId,
		Origin:    anchorReq.Origin,
		Id:        anchorReq.Id.String(),
		Cid:       anchorReq.Cid,
		Timestamp: anchorReq.Timestamp,
		CreatedAt: anchorReq.CreatedAt,
	}
	// If there's an error in any step after the tip update (viz. storing stream/CID, sending status or ready messages),
	// and we return an error, this request will get reprocessed. When that happens, we'll retry updating the tip and
	// that will pass through because we update tips with timestamps greater than *or equal to* the current tip
	// timestamp. If we legitimately found a newer tip that had been written before the older tip was retried, then
	// we'll correctly mark the old tip REPLACED.
	//
	// The greater-than-or-equal-to check also covers the case when two requests have the same timestamp though that is
	// very unlikely given that the timestamps have millisecond resolution.
	//
	// If there's an error marking the old tip REPLACED, the old tip will get anchored along with the new tip, causing
	// the new tip to be rejected in Ceramic via conflict resolution. While not ideal, this is no worse than what we
	// have today.
	//
	// NOTE: A reprocessed request is the *exact* same request processed previously, i.e. it also has the same request
	// UUID. Even though we're letting through reprocessed requests, this does not leave us vulnerable to a malicious
	// actor spamming us with duplicate requests because each API request will arrive at the validation service with a
	// new UUID generated by the Anchor DB.
	if storedTip, oldTip, err := v.stateDb.UpdateTip(ctx, newTip); err != nil {
		log.Printf("validate: failed to store tip: %v, %v", anchorReq, err)
		return err
	} else if !storedTip {
		// Mark the current request REPLACED because we found a newer stream/origin timestamp in the DB
		v.metricService.Count(ctx, models.MetricName_ReplacedRequest, 1)
		return v.sendStatusMsg(ctx, anchorReq.Id, models.RequestStatus_Replaced)
	} else {
		// Having the same request ID means that a newer tip failed to get completely processed previously
		isReprocessedTip := (oldTip != nil) && (oldTip.Id == newTip.Id)
		// Only mark the old tip REPLACED if it actually was an older tip and not just the same tip retried
		if (oldTip != nil) && !isReprocessedTip {
			requestId, _ := uuid.Parse(oldTip.Id)
			v.metricService.Count(ctx, models.MetricName_ReplacedRequest, 1)
			if err = v.sendStatusMsg(ctx, requestId, models.RequestStatus_Replaced); err != nil {
				return err
			}
		}
		if storedCid, err := v.stateDb.StoreCid(ctx, streamCid); err != nil {
			log.Printf("validate: failed to store cid: %v, %v", anchorReq, err)
			return err
		} else if !storedCid && !isReprocessedTip {
			// Mark the current request REPLACED if we found the stream/CID in the DB and this was not a reprocessed
			// tip. A reprocessed request's stream/CID might already have been stored in the DB, and we don't want to
			// skip this request because we know it wasn't fully processed the last time it came through.
			v.metricService.Count(ctx, models.MetricName_ReplacedRequest, 1)
			return v.sendStatusMsg(ctx, anchorReq.Id, models.RequestStatus_Replaced)
		} else
		// This request has been fully de-duplicated so send it to the next stage
		if _, err = v.readyPublisher.SendMessage(ctx, anchorReq); err != nil {
			log.Printf("validate: failed to send ready message: %v, %v", anchorReq, err)
			return err
		}
	}
	return nil
}

func (v ValidationService) sendStatusMsg(ctx context.Context, id uuid.UUID, status models.RequestStatus) error {
	statusMsg := &models.RequestStatusMessage{Id: id, Status: status}
	if _, err := v.statusPublisher.SendMessage(ctx, statusMsg); err != nil {
		log.Printf("validate: failed to send status message: %v, %v", statusMsg, err)
		return err
	}
	return nil
}
