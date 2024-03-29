package services

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ceramicnetwork/go-cas/common/loggers"
	"github.com/ceramicnetwork/go-cas/models"
)

func TestPublishNewTip(t *testing.T) {
	anchorRequest, encodedRequest, streamTip, streamCid, statusMessage := generateTestData(uuid.New(), "streamId", "cid", "origin", 0)
	testCtx := context.Background()
	logger := loggers.NewTestLogger()

	t.Run("publish request if tip does not already exist", func(t *testing.T) {
		stateRepo := &MockStateRepository{}
		readyPublisher := &MockPublisher{messages: make(chan any, 1)}
		metricService := &MockMetricService{}

		validator := NewValidationService(logger, stateRepo, readyPublisher, nil, metricService)
		if err := validator.Validate(testCtx, encodedRequest); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		receivedMessage := waitForMesssages(readyPublisher.messages, 1)[0]
		testAnchorRequest(t, receivedMessage, anchorRequest)
		Assert(t, 1, metricService.counts[models.MetricName_ValidateIngressRequest], "Incorrect ingress request count")
		Assert(t, 0, metricService.counts[models.MetricName_ValidateReprocessedRequest], "Incorrect reprocessed request count")
		Assert(t, 0, metricService.counts[models.MetricName_ValidateReplacedRequest], "Incorrect replaced request count")
		Assert(t, 1, metricService.counts[models.MetricName_ValidateProcessedRequest], "Incorrect processed request count")
	})

	t.Run("publish request and replace old tip", func(t *testing.T) {
		newAnchorRequest, newEncodedRequest, _, _, _ := generateTestData(uuid.New(), "streamId", "newCid", "origin", time.Millisecond)

		stateRepo := &MockStateRepository{}
		readyPublisher := &MockPublisher{messages: make(chan any, 1)}
		statusPublisher := &MockPublisher{messages: make(chan any, 1)}
		metricService := &MockMetricService{}

		stateRepo.UpdateTip(testCtx, streamTip)
		stateRepo.StoreCid(testCtx, streamCid)

		validator := NewValidationService(logger, stateRepo, readyPublisher, statusPublisher, metricService)
		if err := validator.Validate(testCtx, newEncodedRequest); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		receivedMessage := waitForMesssages(readyPublisher.messages, 1)[0]
		if validatedRequest, ok := receivedMessage.(*models.AnchorRequestMessage); !ok {
			t.Fatalf("received invalid anchor request message: %v", receivedMessage)
		} else {
			if !reflect.DeepEqual(validatedRequest, newAnchorRequest) {
				t.Errorf("incorrect request published: expected [%v], got [%v]", anchorRequest, validatedRequest)
			}
		}
		receivedMessage = waitForMesssages(statusPublisher.messages, 1)[0]
		testStatusMessage(t, receivedMessage, statusMessage)
		Assert(t, 1, metricService.counts[models.MetricName_ValidateIngressRequest], "Incorrect ingress request count")
		Assert(t, 0, metricService.counts[models.MetricName_ValidateReprocessedRequest], "Incorrect reprocessed request count")
		Assert(t, 1, metricService.counts[models.MetricName_ValidateReplacedRequest], "Incorrect replaced request count")
		Assert(t, 1, metricService.counts[models.MetricName_ValidateProcessedRequest], "Incorrect processed request count")
	})
}

func TestPublishOldTip(t *testing.T) {
	_, _, streamTip, streamCid, _ := generateTestData(uuid.New(), "streamId", "cid", "origin", 0)
	testCtx := context.Background()
	logger := loggers.NewTestLogger()

	t.Run("do not publish request if tip is older", func(t *testing.T) {
		_, oldEncodedRequest, _, _, newStatusMessage := generateTestData(uuid.New(), "streamId", "newCid", "origin", -time.Millisecond)

		stateRepo := &MockStateRepository{}
		statusPublisher := &MockPublisher{messages: make(chan any, 1)}
		metricService := &MockMetricService{}

		stateRepo.UpdateTip(testCtx, streamTip)
		stateRepo.StoreCid(testCtx, streamCid)

		validator := NewValidationService(logger, stateRepo, nil, statusPublisher, metricService)
		if err := validator.Validate(testCtx, oldEncodedRequest); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		receivedMessage := waitForMesssages(statusPublisher.messages, 1)[0]
		testStatusMessage(t, receivedMessage, newStatusMessage)
		Assert(t, 1, metricService.counts[models.MetricName_ValidateIngressRequest], "Incorrect ingress request count")
		Assert(t, 0, metricService.counts[models.MetricName_ValidateReprocessedRequest], "Incorrect reprocessed request count")
		Assert(t, 1, metricService.counts[models.MetricName_ValidateReplacedRequest], "Incorrect replaced request count")
		Assert(t, 0, metricService.counts[models.MetricName_ValidateProcessedRequest], "Incorrect processed request count")
	})
}

func TestReprocessTips(t *testing.T) {
	anchorRequest, encodedRequest, streamTip, streamCid, _ := generateTestData(uuid.New(), "streamId", "cid", "origin", 0)
	testCtx := context.Background()
	logger := loggers.NewTestLogger()

	t.Run("publish reprocessed request if tip and cid exist", func(t *testing.T) {
		stateRepo := &MockStateRepository{}
		readyPublisher := &MockPublisher{messages: make(chan any, 1)}
		metricService := &MockMetricService{}

		stateRepo.UpdateTip(testCtx, streamTip)
		stateRepo.StoreCid(testCtx, streamCid)

		validatorService := NewValidationService(logger, stateRepo, readyPublisher, nil, metricService)
		if err := validatorService.Validate(testCtx, encodedRequest); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		receivedMessage := waitForMesssages(readyPublisher.messages, 1)[0]
		testAnchorRequest(t, receivedMessage, anchorRequest)
		Assert(t, 1, metricService.counts[models.MetricName_ValidateIngressRequest], "Incorrect ingress request count")
		Assert(t, 1, metricService.counts[models.MetricName_ValidateReprocessedRequest], "Incorrect reprocessed request count")
		Assert(t, 0, metricService.counts[models.MetricName_ValidateReplacedRequest], "Incorrect replaced request count")
		Assert(t, 1, metricService.counts[models.MetricName_ValidateProcessedRequest], "Incorrect processed request count")
	})

	t.Run("publish reprocessed request if tip exists but cid does not", func(t *testing.T) {
		stateRepo := &MockStateRepository{}
		readyPublisher := &MockPublisher{messages: make(chan any, 1)}
		metricService := &MockMetricService{}

		stateRepo.UpdateTip(testCtx, streamTip)

		validatorService := NewValidationService(logger, stateRepo, readyPublisher, nil, metricService)
		if err := validatorService.Validate(testCtx, encodedRequest); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		receivedMessage := waitForMesssages(readyPublisher.messages, 1)[0]
		testAnchorRequest(t, receivedMessage, anchorRequest)
		Assert(t, 1, metricService.counts[models.MetricName_ValidateIngressRequest], "Incorrect ingress request count")
		Assert(t, 1, metricService.counts[models.MetricName_ValidateReprocessedRequest], "Incorrect reprocessed request count")
		Assert(t, 0, metricService.counts[models.MetricName_ValidateReplacedRequest], "Incorrect replaced request count")
		Assert(t, 1, metricService.counts[models.MetricName_ValidateProcessedRequest], "Incorrect processed request count")
	})
}

func TestCidExists(t *testing.T) {
	_, encodedRequest, streamTip, streamCid, statusMessage := generateTestData(uuid.New(), "streamId", "cid", "origin", 0)
	testCtx := context.Background()
	logger := loggers.NewTestLogger()

	t.Run("do not publish request if tip does not exist but cid does", func(t *testing.T) {
		stateRepo := &MockStateRepository{}
		statusPublisher := &MockPublisher{messages: make(chan any, 1)}
		metricService := &MockMetricService{}

		stateRepo.StoreCid(testCtx, streamCid)

		validatorService := NewValidationService(logger, stateRepo, nil, statusPublisher, metricService)
		if err := validatorService.Validate(testCtx, encodedRequest); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		receivedMessage := waitForMesssages(statusPublisher.messages, 1)[0]
		testStatusMessage(t, receivedMessage, statusMessage)
		Assert(t, 1, metricService.counts[models.MetricName_ValidateIngressRequest], "Incorrect ingress request count")
		Assert(t, 0, metricService.counts[models.MetricName_ValidateReprocessedRequest], "Incorrect reprocessed request count")
		Assert(t, 1, metricService.counts[models.MetricName_ValidateReplacedRequest], "Incorrect replaced request count")
		Assert(t, 0, metricService.counts[models.MetricName_ValidateProcessedRequest], "Incorrect processed request count")
	})

	t.Run("replace tip if cid exists", func(t *testing.T) {
		stateRepo := &MockStateRepository{}
		statusPublisher := &MockPublisher{messages: make(chan any, 1)}
		metricService := &MockMetricService{}

		stateRepo.StoreCid(testCtx, streamCid)

		validatorService := NewValidationService(logger, stateRepo, nil, statusPublisher, metricService)
		if err := validatorService.Validate(testCtx, encodedRequest); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		receivedMessage := waitForMesssages(statusPublisher.messages, 1)[0]
		testStatusMessage(t, receivedMessage, statusMessage)
		Assert(t, 1, metricService.counts[models.MetricName_ValidateIngressRequest], "Incorrect ingress request count")
		Assert(t, 0, metricService.counts[models.MetricName_ValidateReprocessedRequest], "Incorrect reprocessed request count")
		Assert(t, 1, metricService.counts[models.MetricName_ValidateReplacedRequest], "Incorrect replaced request count")
		Assert(t, 0, metricService.counts[models.MetricName_ValidateProcessedRequest], "Incorrect processed request count")
	})

	t.Run("replace newer and older tips if cid exists", func(t *testing.T) {
		_, newEncodedRequest, _, _, newStatusMessage := generateTestData(uuid.New(), "streamId", "cid", "origin", time.Millisecond)

		stateRepo := &MockStateRepository{}
		statusPublisher := &MockPublisher{messages: make(chan any, 2)}
		metricService := &MockMetricService{}

		stateRepo.UpdateTip(testCtx, streamTip)
		stateRepo.StoreCid(testCtx, streamCid)

		validatorService := NewValidationService(logger, stateRepo, nil, statusPublisher, metricService)
		if err := validatorService.Validate(testCtx, newEncodedRequest); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		receivedMessages := waitForMesssages(statusPublisher.messages, 2)
		testStatusMessage(t, receivedMessages[0], statusMessage)
		testStatusMessage(t, receivedMessages[1], newStatusMessage)
		Assert(t, 1, metricService.counts[models.MetricName_ValidateIngressRequest], "Incorrect ingress request count")
		Assert(t, 0, metricService.counts[models.MetricName_ValidateReprocessedRequest], "Incorrect reprocessed request count")
		Assert(t, 2, metricService.counts[models.MetricName_ValidateReplacedRequest], "Incorrect replaced request count")
		Assert(t, 0, metricService.counts[models.MetricName_ValidateProcessedRequest], "Incorrect processed request count")
	})
}

func testAnchorRequest(t *testing.T, testMessage any, testRequest *models.AnchorRequestMessage) {
	if requestMessage, ok := testMessage.(*models.AnchorRequestMessage); !ok {
		t.Fatalf("received invalid anchor request message: %v", testMessage)
	} else {
		if !reflect.DeepEqual(requestMessage, testRequest) {
			t.Errorf("incorrect anchor request published: expected [%v], got [%v]", testRequest, requestMessage)
		}
	}
}

func testStatusMessage(t *testing.T, testMessage any, testStatus *models.RequestStatusMessage) {
	if statusMessage, ok := testMessage.(*models.RequestStatusMessage); !ok {
		t.Fatalf("received invalid status message: %v", testMessage)
	} else {
		if !reflect.DeepEqual(statusMessage, testStatus) {
			t.Errorf("incorrect status published: expected [%v], got [%v]", testStatus, statusMessage)
		}
	}
}

func generateTestData(id uuid.UUID, streamId, cid, origin string, delta time.Duration) (*models.AnchorRequestMessage, string, *models.StreamTip, *models.StreamCid, *models.RequestStatusMessage) {
	now := time.Now().Round(0).Add(delta).UTC()
	anchorRequest := &models.AnchorRequestMessage{
		Id:        id,
		StreamId:  streamId,
		Cid:       cid,
		Origin:    origin,
		Timestamp: now,
		CreatedAt: now,
	}
	encodedRequest, _ := json.Marshal(anchorRequest)
	return anchorRequest,
		string(encodedRequest),
		&models.StreamTip{
			StreamId:  anchorRequest.StreamId,
			Origin:    anchorRequest.Origin,
			Id:        anchorRequest.Id.String(),
			Cid:       anchorRequest.Cid,
			Timestamp: anchorRequest.Timestamp,
			CreatedAt: anchorRequest.CreatedAt,
		}, &models.StreamCid{
			StreamId:  anchorRequest.StreamId,
			Cid:       anchorRequest.Cid,
			CreatedAt: anchorRequest.CreatedAt,
		}, &models.RequestStatusMessage{
			Id:     anchorRequest.Id,
			Status: models.RequestStatus_Replaced,
		}
}
