package ipfs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
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
const defaultIpfsPublishPubsubTimeoutS = 30 * time.Second

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

func parseAddress(addrStr string) (ma.Multiaddr, error) {
	if addr, err := ma.NewMultiaddr(addrStr); err == nil {
		return addr, nil
	}

	u, err := url.Parse(addrStr)
	if err != nil {
		return nil, fmt.Errorf("invalid address %s given: %w", addrStr, err)
	}

	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return nil, fmt.Errorf("invalid address %s given: %w", addrStr, err)
	}

	if addr, err := ma.NewMultiaddr(fmt.Sprintf("/dns4/%s/tcp/%s", host, port)); err != nil {
		return nil, fmt.Errorf("unable to get valid multiaddr from %s: %w", addrStr, err)
	} else {
		return addr, nil
	}
}

func NewIpfsApi(logger models.Logger, addrStr string, metricService models.MetricService) *IpfsApi {
	multiaddr, err := parseAddress(addrStr)
	if err != nil {
		logger.Fatalf("Error creating multiaddress for %s: %v", addrStr, err)
	}

	coreApi, err := rpc.NewApi(multiaddr)
	if err != nil {
		logger.Fatalf("Error creating ipfs client at %s: %v", multiaddr.String(), err)
	}

	return NewIpfsApiWithCore(logger, multiaddr.String(), coreApi, metricService)
}

func (i *IpfsApi) Publish(ctx context.Context, topic string, data []byte) error {
	if _, err := i.limiter.Submit(ctx, models.PubSubPublishTask{Topic: topic, Data: data}); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			i.metricService.Count(ctx, models.MetricName_IpfsPubsubPublishExpired, 1)
			return nil
		}
		return err
	}

	return nil
}

func (i *IpfsApi) pubsubPublish(ctx context.Context, task models.PubSubPublishTask) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Duration(defaultIpfsPublishPubsubTimeoutS))
	defer cancel()

	i.logger.Debugf("publishing %v to ipfs pubsub on ipfs %v", task, i.multiAddressStr)
	if err := i.core.PubSub().Publish(ctxWithTimeout, task.Topic, task.Data); err != nil {
		return fmt.Errorf("publishing message to pubsub failed on ipfs instance at %s: %w", i.multiAddressStr, err)
	}
	return nil
}

func (i *IpfsApi) processIpfsTask(ctx context.Context, task any) error {
	if pubsubPublishTask, ok := task.(models.PubSubPublishTask); ok {
		return i.pubsubPublish(ctx, pubsubPublishTask)
	}
	return fmt.Errorf("unknown ipfs task received %v", task)
}

func (i *IpfsApi) limiterRunFunction(ctx context.Context, task any) (any, error) {
	if err := i.processIpfsTask(ctx, task); err != nil {
		i.logger.Errorf("IPFS error for task %v on ipfs %v", task, i.multiAddressStr)
		i.metricService.Count(ctx, models.MetricName_IpfsError, 1)
		// TODO: if we get many timeout errors, adjust the limiter values
		// TODO: restart the ipfs instance if there are too many errors and mark unavailable
		return nil, err
	}

	return nil, nil
}
