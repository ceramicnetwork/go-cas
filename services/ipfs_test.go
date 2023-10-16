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

	"github.com/ceramicnetwork/go-cas/common/ipfs"
	"github.com/ceramicnetwork/go-cas/common/loggers"
	"github.com/ceramicnetwork/go-cas/models"
)

func TestPubsub(t *testing.T) {
	tests := map[string]struct {
		Ctx           context.Context
		Msg           models.IpfsPubsubPublishMessage
		ShouldPublish bool
	}{
		"Can publish pubsub messages": {
			Ctx: context.Background(),
			Msg: models.IpfsPubsubPublishMessage{
				CreatedAt: time.Now(),
				Topic:     "/go-cas/tests",
				Data:      []byte("cats"),
			},
			ShouldPublish: true,
		},
		"Will not publish message if task expired": {
			Ctx: context.Background(),
			Msg: models.IpfsPubsubPublishMessage{
				CreatedAt: time.Now().Add(-time.Minute),
				Topic:     "/go-cas/tests",
				Data:      []byte("cats"),
			},
			ShouldPublish: false,
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
			ipfsInstance := ipfs.NewIpfsApiWithCore(logger, "test", mockCoreApi, metricService)
			ipfsService.ipfsInstances = []iface.CoreAPI{ipfsInstance}

			encodedMsg, err := json.Marshal(test.Msg)
			if err != nil {
				t.Fatalf("Failed to encode msg %v: %v", test.Msg, err)
			}

			err = ipfsService.Run(test.Ctx, string(encodedMsg))

			if err != nil {
				t.Fatalf("failed publishing msg: %v", err)
			}

			if test.ShouldPublish {
				Assert(t, 1, len(mockCoreApi.pubsubApi.publishedMessages), "did not publish correct number of messages")

				receivedMessage := mockCoreApi.pubsubApi.publishedMessages[0]
				if !reflect.DeepEqual(receivedMessage.Data, test.Msg.Data) {
					t.Fatalf("Published incorrect message data, expected: %v, received: %v", test.Msg.Data, receivedMessage.Data)
				}
				if receivedMessage.Topic != test.Msg.Topic {
					t.Fatalf("Published to incorrect topic, expected: %v, received: %v", test.Msg.Topic, receivedMessage.Topic)
				}
			} else {
				Assert(t, 0, len(mockCoreApi.pubsubApi.publishedMessages), "should not have published any messages")
				Assert(t, 1, metricService.counts[models.MetricName_IpfsPubsubPublishExpired], "incorrect expired tasks counted")
			}

			Assert(t, 0, metricService.counts[models.MetricName_IpfsError], "incorrect ipfs errors counted")

		})
	}
}

func TestIPFSUnsupportedTask(t *testing.T) {
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
	ipfsInstance := ipfs.NewIpfsApiWithCore(logger, "test", mockCoreApi, metricService)
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
	ipfsInstance1 := ipfs.NewIpfsApiWithCore(logger, "test", mockCoreApi1, metricService)
	mockCoreApi2 := NewMockIpfsCoreApi(mockNode)
	ipfsInstance2 := ipfs.NewIpfsApiWithCore(logger, "test", mockCoreApi2, metricService)
	mockCoreApi3 := NewMockIpfsCoreApi(mockNode)
	ipfsInstance3 := ipfs.NewIpfsApiWithCore(logger, "test", mockCoreApi3, metricService)
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
