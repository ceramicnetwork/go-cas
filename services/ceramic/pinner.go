package ceramic

import (
	"context"

	"github.com/ceramicnetwork/go-cas/models"
)

type Pinner struct {
	client models.CeramicClient
}

func NewCeramicPinner(client models.CeramicClient) *Pinner {
	return &Pinner{client: client}
}

func (p Pinner) Pin(ctx context.Context, query *models.CeramicPin) error {
	return p.client.Pin(ctx, query)
}
