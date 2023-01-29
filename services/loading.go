package services

import (
	"context"
	"encoding/json"
	"log"

	"github.com/google/uuid"

	"github.com/smrz2001/go-cas/models"
	"github.com/smrz2001/go-cas/services/ceramic"
)

type LoadingService struct {
	ceramicLoader   *ceramic.Loader
	loadPublisher   models.QueuePublisher // The service's own queue to handle multiqueries for CIDs not found normally
	statusPublisher models.QueuePublisher
	stateDb         models.StateRepository
}

func NewLoadingService(
	client models.CeramicClient,
	loadPublisher, statusPublisher models.QueuePublisher,
	stateDb models.StateRepository,
) *LoadingService {
	return &LoadingService{
		ceramic.NewCeramicLoader(client, stateDb),
		loadPublisher,
		statusPublisher,
		stateDb,
	}
}

// Load is the Loading service's message handler. It will be invoked for messages received on either the Load or the
// Multiquery queues. The queue plumbing takes care of scaling the consumers, batching, etc.
//
// - Post an update to the Status queue and delete the request from the loading queue(s) if Ceramic loading succeeds,
//   whether the CID is found or not.
// - If a CID is not found during initial loading, post a new request to the Multiquery queue.
// - For errors posting to SQS, just return the error. This will cause the request to not be deleted from the queue, and
//   then to be reprocessed after the SQS visibility timeout.
func (l LoadingService) Load(ctx context.Context, msgBody string) error {
	anchorReq := new(models.AnchorRequestMessage)
	if err := json.Unmarshal([]byte(msgBody), anchorReq); err != nil {
		return err
	}
	query := &models.CeramicQuery{
		Id:         anchorReq.Id,
		StreamId:   anchorReq.StreamId,
		Cid:        anchorReq.Cid,
		GenesisCid: anchorReq.GenesisCid,
		StreamType: anchorReq.StreamType,
	}
	if queryResult, err := l.ceramicLoader.Query(ctx, query); err != nil {
		log.Printf("load: error loading query=%+v: %v", query, err)
		// Post a failure to the Status queue and delete the request from the loading queue(s)
		return l.resolve(ctx, query.Id, false)
	} else if queryResult.CidFound {
		// Post a successful update to the Status queue and delete the request from the loading queue(s)
		return l.resolve(ctx, query.Id, true)
	} else if queryResult.StreamState != nil {
		// If the request to Ceramic was successful but we didn't find the CID we were looking for, send a multiquery to
		// see if we can find newer commits. Add the genesis CID and stream type to the request so that we can construct
		// a CID-specific multiquery.
		anchorReq.GenesisCid = &queryResult.StreamState.Log[0].Cid
		anchorReq.StreamType = &queryResult.StreamState.Type
		if _, err = l.loadPublisher.SendMessage(ctx, anchorReq); err != nil {
			// Return the error so that the request is not deleted from the Load queue and gets reprocessed after the
			// SQS visibility timeout.
			return err
		}
		// We can delete the event from the Load queue once a new event has been posted to the Multiquery queue
		return nil
	} else {
		// We didn't find the CID even after a multiquery so post a failure to the Status queue and delete the request
		// from the loading queue(s).
		return l.resolve(ctx, query.Id, false)
	}
}

func (l LoadingService) resolve(ctx context.Context, id uuid.UUID, loaded bool) error {
	_, err := l.statusPublisher.SendMessage(ctx, models.RequestStatusMessage{Id: id, Loaded: loaded})
	return err
}
