package services

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-playground/validator"

	"github.com/ceramicnetwork/go-cas/common/ipfs"
	"github.com/ceramicnetwork/go-cas/models"
)

type IpfsService struct {
	metricService models.MetricService
	logger        models.Logger
	ipfsInstances []models.IpfsApi
	next          uint32
	validator     *validator.Validate
}

func NewIpfsService(logger models.Logger, metricService models.MetricService) *IpfsService {
	ipfsService := &IpfsService{
		metricService: metricService,
		logger:        logger,
		validator:     validator.New(),
	}

	ipfsStrAddresses := []string{"/ip4/127.0.0.1/tcp/5011"}
	if configIpfsStrAddresses, found := os.LookupEnv("IPFS_ADDRESSES"); found {
		parsedIpfsStrAddress := strings.Split(configIpfsStrAddresses, " ")
		ipfsStrAddresses = parsedIpfsStrAddress
	}
	var ipfsInstances []models.IpfsApi
	for _, strAddr := range ipfsStrAddresses {
		ipfsApi := ipfs.NewIpfsApi(logger, strAddr, metricService)

		ipfsInstances = append(ipfsInstances, ipfsApi)
	}

	ipfsService.ipfsInstances = ipfsInstances

	return ipfsService
}

// when an error is returned the msg is not acked. It has a visibility timeout set (default 5 min) then it will be retried. If retried too many times it with go to the DLQ
func (i *IpfsService) Run(ctx context.Context, msgBody string) error {
	// TODO: using the data received from the queue message to figure out which ipfs instance to use (id + retry)
	nextIndex := atomic.AddUint32(&i.next, 1)
	ipfsInstance := i.ipfsInstances[(int(nextIndex)-1)%len(i.ipfsInstances)]
	// TODO: skip the current ipfsInstance if it is unavailable
	i.logger.Debugf("received ipfs task request %v", msgBody)
	// Unmarshal into one of the known ipfs messages. Currently there is only one
	pubsubPublishArgs := new(models.IpfsPubsubPublishMessage)
	if err := json.Unmarshal([]byte(msgBody), &pubsubPublishArgs); err == nil {
		if err := i.validator.Struct(pubsubPublishArgs); err == nil {
			i.logger.Debugf("received ipfs pubsub publish request %v", pubsubPublishArgs)
			expiration := pubsubPublishArgs.CreatedAt.Add(time.Minute)
			ctxWithDeadline, cancel := context.WithDeadline(ctx, expiration)
			defer cancel()
			err := ipfsInstance.Publish(ctxWithDeadline, pubsubPublishArgs.Topic, pubsubPublishArgs.Data)
			return err
		}
	}

	i.metricService.Count(ctx, models.MetricName_IpfsUnknownTask, 1)
	return fmt.Errorf("unknown ipfs request message retrieved from the queue %v", msgBody)

}
