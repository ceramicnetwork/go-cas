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

type IpfsApi struct {
	core            iface.CoreAPI
	logger          models.Logger
	multiAddressStr string
	metricService   models.MetricService
	limiter         *ratelimiter.RateLimiter[any, any]
}

func NewIpfsApiWithCore(logger models.Logger, multiAddressStr string, coreApi iface.CoreAPI, metricService models.MetricService) *IpfsApi {
	ipfs := IpfsApi{core: coreApi, logger: logger, multiAddressStr: multiAddressStr, metricService: metricService}
	limiterOpts := ratelimiter.Opts{
		Limit:             defaultIpfsRateLimit,
		Burst:             defaultIpfsBurstLimit,
		MaxQueueDepth:     defaultIpfsLimiterMaxQueueDepth,
		FullQueueStrategy: ratelimiter.BlockWhenFull,
	}
	ipfs.limiter = ratelimiter.New(limiterOpts, ipfs.limiterRunFunction)

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

func (i *IpfsApi) Publish(ctx context.Context, topic string, data []byte) error {
	if _, err := i.limiter.Submit(ctx, PubSubPublishTask{Topic: topic, Data: data}); err != nil {
		if ctx.Err() != nil {
			i.metricService.Count(ctx, models.MetricName_IpfsPubsubPublishExpired, 1)
			return nil
		}
		return err
	}

	return nil
}

func (i *IpfsApi) limiterRunFunction(ctx context.Context, task any) (any, error) {
	var ipfsError error
	var result any
	if pubsubPublishTask, ok := task.(PubSubPublishTask); ok {
		// set a default timeout
		ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Duration(time.Second*time.Duration(defaultIpfsPublishPubsubTimeoutS)))
		defer cancel()

		i.logger.Debugf("publishing %v to ipfs pubsub on ipfs %v", pubsubPublishTask, i.multiAddressStr)
		if err := i.core.PubSub().Publish(ctxWithTimeout, pubsubPublishTask.Topic, pubsubPublishTask.Data); err != nil {
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
