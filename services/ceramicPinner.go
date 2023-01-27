package services

import (
	"context"

	"github.com/abevier/tsk/ratelimiter"

	"github.com/smrz2001/go-cas/models"
)

type ceramicPinner struct {
	client      ceramicClient
	rateLimiter *ratelimiter.RateLimiter[*models.CeramicPin, *models.CeramicPinResult]
}

func NewCeramicPinner(client ceramicClient) *ceramicPinner {
	pinner := ceramicPinner{client: client}
	rlOpts := ratelimiter.Opts{
		Limit:             models.DefaultRateLimit,
		Burst:             models.DefaultRateLimit,
		MaxQueueDepth:     models.DefaultQueueDepthLimit,
		FullQueueStrategy: ratelimiter.BlockWhenFull,
	}
	pinner.rateLimiter = ratelimiter.New(rlOpts, func(ctx context.Context, pin *models.CeramicPin) (*models.CeramicPinResult, error) {
		return client.Pin(ctx, pin.StreamId)
	})
	return &pinner
}

func (p ceramicPinner) Pin(ctx context.Context, query *models.CeramicPin) (*models.CeramicPinResult, error) {
	return p.rateLimiter.Submit(ctx, query)
}
