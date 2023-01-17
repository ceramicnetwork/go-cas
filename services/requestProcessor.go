package services

import (
	"context"
	"encoding/json"

	"github.com/smrz2001/go-cas/models"
)

type LoadingService struct {
	ceramicLoader    *ceramicLoader
	mqPublisher      queuePublisher
	readyPublisher   queuePublisher
	failurePublisher queuePublisher
	stateDb          stateRepository
}

func NewLoadingService(
	client ceramicClient,
	mqPublisher, readyPublisher, failurePublisher queuePublisher,
	stateDb stateRepository,
) *LoadingService {
	return &LoadingService{
		NewCeramicLoader(client, stateDb),
		mqPublisher,
		readyPublisher,
		failurePublisher,
		stateDb,
	}
}

// ProcessQuery is the Loading service's message handler. It will be invoked for messages received on either the Request
// or the Multiquery queues. The queue plumbing takes care of scaling the consumers, batching, etc.
//
// Returning errors from here will cause the anchor request to not be deleted from the originating queue. This is ok
// because it's better to reprocess a request and potentially anchor the corresponding stream more than once than to not
// anchor it at all in case there was some error or a bug in the code that caused the processing failure. Conversely,
// not returning an error will cause the event to get deleted from the originating queue.
func (l LoadingService) ProcessQuery(ctx context.Context, msgBody string) error {
	anchorReq := new(models.AnchorRequestMessage)
	err := json.Unmarshal([]byte(msgBody), anchorReq)
	if err != nil {
		return err
	}
	query := &models.CeramicQuery{
		StreamId:   anchorReq.StreamId,
		Cid:        anchorReq.Cid,
		GenesisCid: anchorReq.GenesisCid,
		StreamType: anchorReq.StreamType,
	}
	var queryResult models.CeramicQueryResult
	queryResult, err = l.ceramicLoader.Submit(ctx, query)
	if err != nil {
		return err
	}
	// If we found commits we haven't encountered before - make sure we anchor this stream, even if we didn't find the
	// specific commit requested.
	if queryResult.Anchor {
		if _, err = l.readyPublisher.SendMessage(ctx, models.Stream{StreamId: query.StreamId}); err != nil {
			return err
		}
	}
	if !queryResult.CidFound {
		// We might have already decided to anchor this stream but if we didn't find the CID we were looking for, send a
		// multiquery to see if we can find newer commits. We don't have to hold up anchoring for this stream to query
		// the missing commit if we already have *some* newer commits to anchor. There's enough deduplication built into
		// downstream components that streams shouldn't get anchored multiple times back-to-back. Even if they do
		// sometimes though, that's ok.
		if queryResult.StreamState != nil {
			// Add the genesis CID and stream type to the query so that we can construct a CID-specific multiquery
			query.GenesisCid = &queryResult.StreamState.Log[0].Cid
			query.StreamType = &queryResult.StreamState.Type
			if _, err = l.mqPublisher.SendMessage(ctx, query); err != nil {
				return err
			}
			// We can delete the event from the Request queue once a new event has been posted to the Multiquery queue
			return nil
		} else {
			// TODO: Don't post to Failure queue if this is a permanent failure like CACAO expiration or rejection via conflict resolution
			if _, err = l.failurePublisher.SendMessage(ctx, query); err != nil {
				return err
			}
		}
	}
	// Delete the anchor request from the queue if we couldn't find the stream at all, or if we did and were able to
	// mark it ready for anchoring.
	return nil
}
