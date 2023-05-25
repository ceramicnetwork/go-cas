package services

import (
	"context"
	"encoding/json"
	"log"

	"github.com/ceramicnetwork/go-cas/models"
)

type ValidationService struct {
	stateDb        models.StateRepository
	readyPublisher models.QueuePublisher
}

func NewValidationService(stateDb models.StateRepository, readyPublisher models.QueuePublisher) *ValidationService {
	return &ValidationService{stateDb, readyPublisher}
}

func (v ValidationService) Validate(ctx context.Context, msgBody string) error {
	anchorReq := new(models.AnchorRequestMessage)
	if err := json.Unmarshal([]byte(msgBody), anchorReq); err != nil {
		return err
	}
	streamCid := models.StreamCid{
		StreamId:  anchorReq.StreamId,
		Cid:       anchorReq.Cid,
		Timestamp: anchorReq.Timestamp,
	}
	if stored, err := v.stateDb.StoreCid(&streamCid); err != nil {
		log.Printf("validate: failed to store message: %v, %v", anchorReq, err)
		return err
	} else if stored {
		// Only post the request to the Ready queue if the CID didn't already exist in the DB
		if _, err = v.readyPublisher.SendMessage(ctx, anchorReq); err != nil {
			log.Printf("validate: failed to send message: %v, %v", anchorReq, err)
			return err
		}
	}
	return nil
}
