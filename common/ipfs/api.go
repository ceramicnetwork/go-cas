package ipfs

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

const defaultIpfsRateLimit = 16
const defaultIpfsBurstLimit = 16
const defaultIpfsLimiterMaxQueueDepth = 100
const defaultIpfsPublishPubsubTimeoutS = 30

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
			return nil
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

func NewIpfsApiWithCore(logger models.Logger, multiAddressStr string, coreApi iface.CoreAPI, metricService models.MetricService) *IpfsApi {
	ipfs := IpfsApi{CoreAPI: coreApi, logger: logger, multiAddressStr: multiAddressStr, metricService: metricService}
	limiterOpts := ratelimiter.Opts{
		Limit:             defaultIpfsRateLimit,
		Burst:             defaultIpfsBurstLimit,
		MaxQueueDepth:     defaultIpfsLimiterMaxQueueDepth,
		FullQueueStrategy: ratelimiter.BlockWhenFull,
	}
	ipfs.limiter = ratelimiter.New(limiterOpts, ipfs.limiterRunFunction)
	ipfs.pubsubApi = &PubSubApi{PubSubAPI: ipfs.CoreAPI.PubSub(), limiter: ipfs.limiter, metricService: ipfs.metricService}

	return &ipfs
}

func NewIpfsApi(logger models.Logger, multiAddressStr string, metricService models.MetricService) *IpfsApi {
	addr, err := ma.NewMultiaddr(multiAddressStr)
	if err != nil {
		logger.Fatalf("Error creating multiaddress for %s: %v", multiAddressStr, err)
	}

	coreApi, err := rpc.NewApi(addr)
	if err != nil {
		logger.Fatalf("Error creating ipfs client at %s: %v", multiAddressStr, err)
	}

	return NewIpfsApiWithCore(logger, multiAddressStr, coreApi, metricService)
}

func (i IpfsApi) PubSub() iface.PubSubAPI {
	return i.pubsubApi
}

func (i *IpfsApi) limiterRunFunction(ctx context.Context, task any) (any, error) {
	var ipfsError error
	var result any
	if pubsubPublishTask, ok := task.(PubSubPublishTask); ok {
		// set a default timeout
		ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Duration(time.Second*time.Duration(defaultIpfsPublishPubsubTimeoutS)))
		defer cancel()

		i.logger.Debugf("publishing %v to ipfs pubsub on ipfs %v", pubsubPublishTask, i.multiAddressStr)
		if err := i.CoreAPI.PubSub().Publish(ctxWithTimeout, pubsubPublishTask.Topic, pubsubPublishTask.Data); err != nil {
			ipfsError = fmt.Errorf("publishing message to pubsub failed on ipfs instance at %s: %v", i.multiAddressStr, err)
		}
	} else {
		return result, fmt.Errorf("unknown ipfs task received %v", task)
	}

	if ipfsError != nil {
		i.metricService.Count(ctx, models.MetricName_IpfsError, 1)

		// TODO: if we get many timeout errors, adjust the limiter values
		// TODO: restart the ipfs instance if there are too many errors and mark unavailable
		return result, ipfsError
	}

	return result, ipfsError
}
