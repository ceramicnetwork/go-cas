package services

import (
	"context"
	"encoding/json"
	"log"

	"github.com/ceramicnetwork/go-cas/models"
	"github.com/ceramicnetwork/go-cas/services/ceramic"
)

type PinningService struct {
	ceramicPinner *ceramic.Pinner
}

func NewPinningService(client models.CeramicClient) *PinningService {
	return &PinningService{ceramic.NewCeramicPinner(client)}
}

// Pin is the Loading service's message handler. It will be invoked for messages received on the Pin queue. The queue
// plumbing takes care of scaling the consumers, batching, etc.
//
// We won't return errors from here, which will cause the pin request to be deleted from the originating queue. Pinning
// is an optimization and not absolutely necessary, so it's ok if it's skipped for a few streams now and then. These
// streams will get loaded again eventually once the request is processed by the loading service.
func (p PinningService) Pin(ctx context.Context, msgBody string) error {
	anchorReq := new(models.AnchorRequestMessage)
	if err := json.Unmarshal([]byte(msgBody), anchorReq); err != nil {
		return err
	}
	pin := &models.CeramicPin{
		StreamId: anchorReq.StreamId,
	}
	if err := p.ceramicPinner.Pin(ctx, pin); err != nil {
		log.Printf("pin: error pinning streamid=%s: %v", pin.StreamId, err)
	}
	return nil
}
