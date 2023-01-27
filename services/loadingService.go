package services

import (
	"context"
	"encoding/json"

	"github.com/smrz2001/go-cas/models"
)

type LoadingService struct {
	ceramicLoader   *ceramicLoader
	loadPublisher   queuePublisher // The service's own queue to handle multiqueries for CIDs not found normally
	statusPublisher queuePublisher
	stateDb         stateRepository
}

func NewLoadingService(
	client ceramicClient,
	loadPublisher, statusPublisher queuePublisher,
	stateDb stateRepository,
) *LoadingService {
	return &LoadingService{
		NewCeramicLoader(client, stateDb),
		loadPublisher,
		statusPublisher,
		stateDb,
	}
}

// Load is the Loading service's message handler. It will be invoked for messages received on either the Load or the
// Multiquery queues. The queue plumbing takes care of scaling the consumers, batching, etc.
//
// Returning errors from here will cause the anchor request to not be deleted from the originating queue. This is ok
// because it's better to reprocess a request and potentially anchor the corresponding stream more than once than to not
// anchor it at all in case there was some error or a bug in the code that caused the processing failure. Conversely,
// not returning an error will cause the event to get deleted from the originating queue.
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
		return err
	} else if queryResult.CidFound {
		// We found the CID!
		if _, err = l.statusPublisher.SendMessage(ctx, models.RequestStatusMessage{Id: query.Id, Loaded: true}); err != nil {
			return err
		}
	} else
	// If we didn't find the CID we were looking for, send a multiquery to see if we can find newer commits.
	if queryResult.StreamState != nil {
		// Add the genesis CID and stream type to the request so that we can construct a CID-specific multiquery
		anchorReq.GenesisCid = &queryResult.StreamState.Log[0].Cid
		anchorReq.StreamType = &queryResult.StreamState.Type
		if _, err = l.loadPublisher.SendMessage(ctx, anchorReq); err != nil {
			return err
		}
		// We can delete the event from the Load queue once a new event has been posted to the Multiquery queue
		return nil
	} else if _, err = l.statusPublisher.SendMessage(ctx, models.RequestStatusMessage{Id: query.Id}); err != nil {
		return err
	}
	// Delete the anchor request from the queue if we were able to resolve the stream status, whether found or not.
	return nil
}
