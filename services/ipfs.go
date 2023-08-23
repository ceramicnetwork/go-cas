package services

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/abevier/tsk/ratelimiter"
	"github.com/ceramicnetwork/go-cas/models"
	"github.com/go-playground/validator"
	iface "github.com/ipfs/boxo/coreiface"
	"github.com/ipfs/kubo/client/rpc"
	ma "github.com/multiformats/go-multiaddr"
)

const DefaultIpfsRateLimit = 16
const DefaultIpfsBurstLimit = 16
const DefaultIpfsLimiterMaxQueueDepth = 100
const DefaultIpfsPublishPubsubTimeoutMs = 30

type PubSubPublishArgs struct {
	Topic     string `json:"topic"  validate:"required"`
	Data      []byte `json:"data"  validate:"required"`
	TimeoutMs int    `json:"timeoutMs,omitempty"`
}

type IpfsApi interface {
	PubSub() iface.PubSubAPI
}

type IpfsWithRateLimiting struct {
	multiAddressStr string
	api             IpfsApi
	limiter         *ratelimiter.RateLimiter[string, any]
	metricService   models.MetricService
	validator       *validator.Validate
}

func NewIpfsWithRateLimiting(logger models.Logger, multiAddressStr string, metricService models.MetricService) *IpfsWithRateLimiting {
	addr, err := ma.NewMultiaddr(multiAddressStr)
	if err != nil {
		logger.Fatalf("Error creating multiaddress for %s: %v", multiAddressStr, err)
	}

	api, err := rpc.NewApi(addr)
	if err != nil {
		logger.Fatalf("Error creating ipfs client at %s: %v", multiAddressStr, err)
	}

	ipfs := IpfsWithRateLimiting{api: api, multiAddressStr: multiAddressStr, metricService: metricService, validator: validator.New()}

	ipfs.withLimiter()

	return &ipfs
}

func (i *IpfsWithRateLimiting) withLimiter() {
	limiterOpts := ratelimiter.Opts{
		Limit:             DefaultIpfsRateLimit,
		Burst:             DefaultIpfsBurstLimit,
		MaxQueueDepth:     DefaultIpfsLimiterMaxQueueDepth,
		FullQueueStrategy: ratelimiter.BlockWhenFull,
	}
	i.limiter = ratelimiter.New(limiterOpts, i.limiterRunFunction)
}

func (i *IpfsWithRateLimiting) limiterRunFunction(ctx context.Context, msgBody string) (any, error) {
	var ipfsError error
	var result any
	var isKnownTask bool

	// Unmarshal into one of the known ipfs tasks. Currently there is only one
	pubsubPublishArgs := new(PubSubPublishArgs)
	if err := json.Unmarshal([]byte(msgBody), &pubsubPublishArgs); err == nil {
		if err := i.validator.Struct(pubsubPublishArgs); err == nil {
			isKnownTask = true
			timeoutMs := DefaultIpfsPublishPubsubTimeoutMs
			if pubsubPublishArgs.TimeoutMs != 0 {
				timeoutMs = pubsubPublishArgs.TimeoutMs
			}

			ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Duration(time.Millisecond*time.Duration(timeoutMs)))
			defer cancel()
			if err := i.api.PubSub().Publish(ctxWithTimeout, pubsubPublishArgs.Topic, pubsubPublishArgs.Data); err != nil {
				ipfsError = fmt.Errorf("publishing message to pubsub failed on ipfs instance at %s: %v", i.multiAddressStr, err)
			}
		}
	}

	if ipfsError != nil {
		i.metricService.Count(ctx, models.MetricName_IpfsError, 1)

		// TODO: if we get many timeout errors, adjust the limiter values
		// TODO: restart the ipfs instance if there are too many errors and mark unavailable
		return result, ipfsError
	}

	if !isKnownTask {
		i.metricService.Count(ctx, models.MetricName_IpfsUnknownTask, 1)
		return result, fmt.Errorf("ipfs task is not supported %v", msgBody)
	}

	return result, ipfsError
}

type IpfsService struct {
	metricService models.MetricService
	logger        models.Logger
	ipfsInstances []*IpfsWithRateLimiting
	next          uint32
}

func NewIpfsService(logger models.Logger, metricService models.MetricService) *IpfsService {
	ipfsService := &IpfsService{
		metricService: metricService,
		logger:        logger,
	}

	ipfsStrAddresses := []string{"/ip4/127.0.0.1/tcp/5011"}
	if configIpfsStrAddresses, found := os.LookupEnv("IPFS_MULTIADDRESSES"); found {
		parsedIpfsStrAddress := strings.Split(configIpfsStrAddresses, " ")
		ipfsStrAddresses = parsedIpfsStrAddress
	}
	var ipfsInstances []*IpfsWithRateLimiting
	for _, strAddr := range ipfsStrAddresses {
		ipfsWithRateLimiting := NewIpfsWithRateLimiting(logger, strAddr, metricService)

		ipfsInstances = append(ipfsInstances, ipfsWithRateLimiting)
	}

	ipfsService.ipfsInstances = ipfsInstances

	return ipfsService
}

func (i *IpfsService) Run(ctx context.Context, msgBody string) error {
	// TODO: using the data received from the queue message to figure out which ipfs instance to use (id + retry)
	nextIndex := atomic.AddUint32(&i.next, 1)
	ipfsInstance := i.ipfsInstances[(int(nextIndex)-1)%len(i.ipfsInstances)]

	// TODO: if the ipfsInstance is iskip the current ipfsInstance if it is unavailable

	_, err := ipfsInstance.limiter.Submit(ctx, msgBody)

	// the msg is not acked. It has a visibility timeout set (default 5 min) then it will be retried. If retried too many times it with go to the DLQ
	return err
}
