package services

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	"github.com/ceramicnetwork/go-cas/common/loggers"
	"github.com/ceramicnetwork/go-cas/models"
	"github.com/go-playground/validator"
)

func TestPubsub(t *testing.T) {
	tests := map[string]struct {
		Ctx         context.Context
		Msg         PubSubPublishArgs
		ShouldError bool
	}{
		"Can publish pubsub messages": {
			Ctx: context.Background(),
			Msg: PubSubPublishArgs{
				Topic: "/go-cas/tests",
				Data:  []byte("cats"),
			},
		},
		"Will timeout if publishing takes too long": {
			Ctx: context.Background(),
			Msg: PubSubPublishArgs{
				Topic:     "/go-cas/tests",
				Data:      []byte("cats"),
				TimeoutMs: 1,
			},
			ShouldError: true,
		},
	}

	logger := loggers.NewTestLogger()

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			metricService := &MockMetricService{}
			ipfsService := NewIpfsService(logger, metricService)
			mockIpfsApi := &MockIpfsApi{}
			ipfsInstance := IpfsWithRateLimiting{multiAddressStr: "test", api: mockIpfsApi, metricService: metricService, validator: validator.New()}
			ipfsInstance.withLimiter()
			ipfsService.ipfsInstances = []*IpfsWithRateLimiting{&ipfsInstance}

			encodedMsg, err := json.Marshal(test.Msg)
			if err != nil {
				t.Fatalf("Failed to encode msg %v: %v", test.Msg, err)
			}

			err = ipfsService.Run(test.Ctx, string(encodedMsg))
			if !test.ShouldError {
				if err != nil {
					t.Fatalf("failed publishing msg: %v", err)
				}
				if len(mockIpfsApi.pubsub.publishedMessages) != 1 {
					t.Fatalf("should have published 1 message, published %v messages", len(mockIpfsApi.pubsub.publishedMessages))
				}
				if !reflect.DeepEqual(mockIpfsApi.pubsub.publishedMessages[0], &test.Msg) {
					t.Fatalf("Published incorrect message,expected: %v, received: %v", test.Msg, mockIpfsApi.pubsub.publishedMessages[0])
				}
				Assert(t, 0, metricService.counts[models.MetricName_IpfsError], "incorrect ipfs errors counted")
			} else {
				if err == nil {
					t.Fatalf("Should have failed publishing msg")
				}
				if len(mockIpfsApi.pubsub.publishedMessages) != 0 {
					t.Fatalf("Should not have published msg %v", mockIpfsApi.pubsub.publishedMessages[0])
				}
				Assert(t, 1, metricService.counts[models.MetricName_IpfsError], "incorrect ipfs errors counted")
			}
		})
	}
}

func TestIPFSUnsupportTask(t *testing.T) {
	logger := loggers.NewTestLogger()
	metricService := &MockMetricService{}
	ipfsService := NewIpfsService(logger, metricService)
	mockIpfsApi := &MockIpfsApi{}
	ipfsInstance := IpfsWithRateLimiting{multiAddressStr: "test", api: mockIpfsApi, metricService: metricService, validator: validator.New()}
	ipfsInstance.withLimiter()
	ipfsService.ipfsInstances = []*IpfsWithRateLimiting{&ipfsInstance}

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

	mockIpfsApi1 := &MockIpfsApi{}
	ipfsInstance1 := IpfsWithRateLimiting{multiAddressStr: "test1", api: mockIpfsApi1, metricService: metricService, validator: validator.New()}
	ipfsInstance1.withLimiter()
	mockIpfsApi2 := &MockIpfsApi{}
	ipfsInstance2 := IpfsWithRateLimiting{multiAddressStr: "test2", api: mockIpfsApi2, metricService: metricService, validator: validator.New()}
	ipfsInstance2.withLimiter()
	mockIpfsApi3 := &MockIpfsApi{}
	ipfsInstance3 := IpfsWithRateLimiting{multiAddressStr: "test3", api: mockIpfsApi3, metricService: metricService, validator: validator.New()}
	ipfsInstance3.withLimiter()
	ipfsService.ipfsInstances = []*IpfsWithRateLimiting{&ipfsInstance1, &ipfsInstance2, &ipfsInstance3}

	encodedMsg, err := json.Marshal(PubSubPublishArgs{
		Topic: "/go-cas/tests",
		Data:  []byte("cats"),
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

	if len(mockIpfsApi1.pubsub.publishedMessages) != 3 {
		t.Fatalf("First ipfs should have published 3 message, published %v messages", len(mockIpfsApi1.pubsub.publishedMessages))
	}
	if len(mockIpfsApi2.pubsub.publishedMessages) != 2 {
		t.Fatalf("Second ipfs should have published 2 message, published %v messages", len(mockIpfsApi2.pubsub.publishedMessages))
	}
	if len(mockIpfsApi3.pubsub.publishedMessages) != 2 {
		t.Fatalf("Third ipfs should have published 2 message, published %v messages", len(mockIpfsApi3.pubsub.publishedMessages))
	}
}
