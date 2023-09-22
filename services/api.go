package services

import (
	"context"
	"fmt"
	"time"

	"github.com/abevier/tsk/ratelimiter"
	iface "github.com/ipfs/boxo/coreiface"
	"github.com/ipfs/kubo/client/rpc"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/ceramicnetwork/go-cas/models"
)

const DefaultIpfsRateLimit = 16
const DefaultIpfsBurstLimit = 16
const DefaultIpfsLimiterMaxQueueDepth = 100
const DefaultIpfsPublishPubsubTimeoutMs = 30

type PubSubPublishTask struct {
	Topic string
	Data  []byte
}

type PubSubApi struct {
	iface.PubSubAPI
	limiter       *ratelimiter.RateLimiter[any, any]
	metricService models.MetricService
}

func (p *PubSubApi) Publish(ctx context.Context, topic string, data []byte) error {
	if _, err := p.limiter.Submit(ctx, PubSubPublishTask{Topic: topic, Data: data}); err == nil {
		// wait here to not flood the pubsub
		time.Sleep(100 * time.Millisecond)
		return nil
	} else {
		if ctx.Err() != nil {
			p.metricService.Count(ctx, models.MetricName_IpfsPubsubPublishExpired, 1)
		}
		return err
	}
}

type IpfsApi struct {
	iface.CoreAPI
	logger          models.Logger
	multiAddressStr string
	metricService   models.MetricService
	limiter         *ratelimiter.RateLimiter[any, any]
	pubsubApi       *PubSubApi
}

func NewIpfsApi(logger models.Logger, multiAddressStr string, metricService models.MetricService) *IpfsApi {
	addr, err := ma.NewMultiaddr(multiAddressStr)
	if err != nil {
		logger.Fatalf("Error creating multiaddress for %s: %v", multiAddressStr, err)
	}

	api, err := rpc.NewApi(addr)
	if err != nil {
		logger.Fatalf("Error creating ipfs client at %s: %v", multiAddressStr, err)
	}

	ipfs := IpfsApi{CoreAPI: api, logger: logger, multiAddressStr: multiAddressStr, metricService: metricService}
	ipfs.withLimiter()

	return &ipfs
}

func (i IpfsApi) PubSub() iface.PubSubAPI {
	return i.pubsubApi
}

func (i *IpfsApi) withLimiter() {
	limiterOpts := ratelimiter.Opts{
		Limit:             DefaultIpfsRateLimit,
		Burst:             DefaultIpfsBurstLimit,
		MaxQueueDepth:     DefaultIpfsLimiterMaxQueueDepth,
		FullQueueStrategy: ratelimiter.BlockWhenFull,
	}
	i.limiter = ratelimiter.New(limiterOpts, i.limiterRunFunction)
	i.pubsubApi = &PubSubApi{PubSubAPI: i.CoreAPI.PubSub(), limiter: i.limiter, metricService: i.metricService}
}

func (i *IpfsApi) limiterRunFunction(ctx context.Context, task any) (any, error) {

	var ipfsError error
	var result any
	if pubsubPublishTask, ok := task.(PubSubPublishTask); ok {
		fmt.Println("is publish task")
		if _, deadlineSet := ctx.Deadline(); !deadlineSet {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Duration(time.Millisecond*time.Duration(DefaultIpfsPublishPubsubTimeoutMs)))
			ctx = ctxWithTimeout
			defer cancel()
		}

		i.logger.Debugf("publishing %v to ipfs pubsub on ipfs %v", pubsubPublishTask, i.multiAddressStr)
		if err := i.CoreAPI.PubSub().Publish(ctx, pubsubPublishTask.Topic, pubsubPublishTask.Data); err != nil {
			ipfsError = fmt.Errorf("publishing message to pubsub failed on ipfs instance at %s: %v", i.multiAddressStr, err)
		}
	} else {
		fmt.Println("wtf")
		return result, fmt.Errorf("unknown ipfs task received %v", task)
	}
	fmt.Println("after it all")

	if ipfsError != nil {
		i.metricService.Count(ctx, models.MetricName_IpfsError, 1)

		// TODO: if we get many timeout errors, adjust the limiter values
		// TODO: restart the ipfs instance if there are too many errors and mark unavailable
		return result, ipfsError
	}

	return result, ipfsError
}
