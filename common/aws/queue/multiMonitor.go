package queue

import (
	"context"

	"github.com/ceramicnetwork/go-cas/models"
)

var _ models.QueueMonitor = &MultiMonitor{}

type MultiMonitor struct {
	monitors []models.QueueMonitor
}

func NewMultiMonitor(monitors []models.QueueMonitor) models.QueueMonitor {
	return &MultiMonitor{monitors}
}

func (m MultiMonitor) GetUtilization(ctx context.Context) (int, int, error) {
	// Get the utilization of all sub-queues
	totalMsgsUnprocessed := 0
	totalMsgsInFlight := 0
	for _, monitor := range m.monitors {
		numMsgsUnprocessed, numMsgsInFlight, err := monitor.GetUtilization(ctx)
		if err != nil {
			return 0, 0, err
		}
		totalMsgsUnprocessed += numMsgsUnprocessed
		totalMsgsInFlight += numMsgsInFlight
	}
	return totalMsgsUnprocessed, totalMsgsInFlight, nil
}
