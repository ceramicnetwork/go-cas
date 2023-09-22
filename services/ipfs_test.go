package services

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	iface "github.com/ipfs/boxo/coreiface"
	"github.com/ipfs/kubo/core"
	ipfsMock "github.com/ipfs/kubo/core/mock"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"github.com/ceramicnetwork/go-cas/common/loggers"
	"github.com/ceramicnetwork/go-cas/models"
)

func TestPubsub(t *testing.T) {
	tests := map[string]struct {
		Ctx              context.Context
		Msg              models.IpfsPubsubPublishMessage
		ShouldError      bool
		ShouldNotPublish bool
	}{
		"Can publish pubsub messages": {
			Ctx: context.Background(),
			Msg: models.IpfsPubsubPublishMessage{
				CreatedAt: time.Now(),
				Topic:     "/go-cas/tests",
				Data:      []byte("cats"),
			},
		},
		"Will timeout if publishing takes too long": {
			Ctx: context.Background(),
			Msg: models.IpfsPubsubPublishMessage{
				CreatedAt: time.Now(),
				Topic:     "/go-cas/tests",
				Data:      []byte("cats"),
				TimeoutMs: 10,
			},
			ShouldError: true,
		},
		"Will not publish message if task expired": {
			Ctx: context.Background(),
			Msg: models.IpfsPubsubPublishMessage{
				CreatedAt: time.Now().Add(-time.Minute),
				Topic:     "/go-cas/tests",
				Data:      []byte("cats"),
			},
			ShouldNotPublish: true,
		},
	}

	logger := loggers.NewTestLogger()

	mockNode, _ := core.NewNode(context.Background(), &core.BuildCfg{
		Online: true,
		Host:   ipfsMock.MockHostOption(mocknet.New()),
		ExtraOpts: map[string]bool{
			"pubsub": true,
		},
	})
	defer mockNode.Close()

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			mockCoreApi := NewMockIpfsCoreApi(mockNode)
			metricService := &MockMetricService{}
			ipfsService := NewIpfsService(logger, metricService)
			ipfsInstance := IpfsApi{multiAddressStr: "test", CoreAPI: mockCoreApi, metricService: metricService, logger: logger}
			ipfsInstance.withLimiter()
			ipfsService.ipfsInstances = []iface.CoreAPI{ipfsInstance}

			encodedMsg, err := json.Marshal(test.Msg)
			if err != nil {
				t.Fatalf("Failed to encode msg %v: %v", test.Msg, err)
			}

			err = ipfsService.Run(test.Ctx, string(encodedMsg))
			if !test.ShouldError {
				if err != nil {
					t.Fatalf("failed publishing msg: %v", err)
				}

				if test.ShouldNotPublish {
					if len(mockCoreApi.pubsubApi.publishedMessages) != 0 {
						t.Fatalf("should have published 0 message, published %v messages", len(mockCoreApi.pubsubApi.publishedMessages))
					}

				} else {
					if len(mockCoreApi.pubsubApi.publishedMessages) != 1 {
						t.Fatalf("should have published 1 message, published %v messages", len(mockCoreApi.pubsubApi.publishedMessages))
					}
					receivedMessage := mockCoreApi.pubsubApi.publishedMessages[0]
					if !reflect.DeepEqual(receivedMessage.Data, test.Msg.Data) {
						t.Fatalf("Published incorrect message data, expected: %v, received: %v", test.Msg.Data, receivedMessage.Data)
					}
					if receivedMessage.Topic != test.Msg.Topic {
						t.Fatalf("Published to incorrect topic, expected: %v, received: %v", test.Msg.Topic, receivedMessage.Topic)
					}
				}

				Assert(t, 0, metricService.counts[models.MetricName_IpfsError], "incorrect ipfs errors counted")
			} else {
				if err == nil {
					t.Fatalf("Should have failed publishing msg")
				}
				if len(mockCoreApi.pubsubApi.publishedMessages) != 0 {
					t.Fatalf("Should not have published msg %v", mockCoreApi.pubsubApi.publishedMessages[0])
				}
				Assert(t, 1, metricService.counts[models.MetricName_IpfsPubsubPublishExpired], "incorrect expired tasks counted")
			}
		})
	}
}

func TestIPFSUnsupportTask(t *testing.T) {
	logger := loggers.NewTestLogger()
	metricService := &MockMetricService{}
	ipfsService := NewIpfsService(logger, metricService)

	mockNode, _ := core.NewNode(context.Background(), &core.BuildCfg{
		Online: true,
		Host:   ipfsMock.MockHostOption(mocknet.New()),
		ExtraOpts: map[string]bool{
			"pubsub": true,
		},
	})
	defer mockNode.Close()
	mockCoreApi := NewMockIpfsCoreApi(mockNode)
	ipfsInstance := IpfsApi{multiAddressStr: "test", CoreAPI: mockCoreApi, metricService: metricService, logger: logger}
	ipfsInstance.withLimiter()
	ipfsService.ipfsInstances = []iface.CoreAPI{ipfsInstance}

	encodedMsg, err := json.Marshal(struct {
		Name   string
		IsGood bool
	}{
		"Rex",
		true,
	})
	if err != nil {
		t.Fatalf("Failed to encode msg: %v", err)
	}
	err = ipfsService.Run(context.Background(), string(encodedMsg))
	if err == nil {
		t.Fatalf("Should have failed because task does not exist")
	}
	Assert(t, 1, metricService.counts[models.MetricName_IpfsUnknownTask], "incorrect ipfs errors counted")

}

func TestIPFSRoundRobin(t *testing.T) {
	logger := loggers.NewTestLogger()
	metricService := &MockMetricService{}
	ipfsService := NewIpfsService(logger, metricService)

	mockNode, _ := core.NewNode(context.Background(), &core.BuildCfg{
		Online: true,
		Host:   ipfsMock.MockHostOption(mocknet.New()),
		ExtraOpts: map[string]bool{
			"pubsub": true,
		},
	})
	defer mockNode.Close()

	mockCoreApi1 := NewMockIpfsCoreApi(mockNode)
	ipfsInstance1 := IpfsApi{multiAddressStr: "test", CoreAPI: mockCoreApi1, metricService: metricService, logger: logger}
	ipfsInstance1.withLimiter()
	mockCoreApi2 := NewMockIpfsCoreApi(mockNode)
	ipfsInstance2 := IpfsApi{multiAddressStr: "test", CoreAPI: mockCoreApi2, metricService: metricService, logger: logger}
	ipfsInstance2.withLimiter()
	mockCoreApi3 := NewMockIpfsCoreApi(mockNode)
	ipfsInstance3 := IpfsApi{multiAddressStr: "test", CoreAPI: mockCoreApi3, metricService: metricService, logger: logger}
	ipfsInstance3.withLimiter()
	ipfsService.ipfsInstances = []iface.CoreAPI{ipfsInstance1, ipfsInstance2, ipfsInstance3}

	encodedMsg, err := json.Marshal(models.IpfsPubsubPublishMessage{
		CreatedAt: time.Now(),
		Topic:     "/go-cas/tests",
		Data:      []byte("cats"),
	})
	if err != nil {
		t.Fatalf("Failed to encode msg: %v", err)
	}

	numCalls := 7
	var wg sync.WaitGroup
	ch := make(chan error, numCalls)
	for i := 0; i < numCalls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := ipfsService.Run(context.Background(), string(encodedMsg))
			if err != nil {
				ch <- err
			}
		}()
	}

	wg.Wait()
	close(ch)

	if len(ch) != 0 {
		err := <-ch
		t.Fatalf("Received error : %v", err)
	}

	if len(mockCoreApi1.pubsubApi.publishedMessages) != 3 {
		t.Fatalf("First ipfs should have published 3 message, published %v messages", len(mockCoreApi1.pubsubApi.publishedMessages))
	}
	if len(mockCoreApi2.pubsubApi.publishedMessages) != 2 {
		t.Fatalf("Second ipfs should have published 2 message, published %v messages", len(mockCoreApi2.pubsubApi.publishedMessages))
	}
	if len(mockCoreApi3.pubsubApi.publishedMessages) != 2 {
		t.Fatalf("Third ipfs should have published 2 message, published %v messages", len(mockCoreApi3.pubsubApi.publishedMessages))
	}
}
