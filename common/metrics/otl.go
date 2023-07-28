package metrics

import (
	"context"
	"encoding/json"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/metric"
	sdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/ceramicnetwork/go-cas"
	"github.com/ceramicnetwork/go-cas/common"
	"github.com/ceramicnetwork/go-cas/models"
)

type OtlMetricService struct {
	meterProvider *sdk.MeterProvider
	meter         metric.Meter
	logger        models.Logger
}

func NewMetricService(ctx context.Context, logger models.Logger) (models.MetricService, error) {
	var resources *resource.Resource
	// Resources are a special type of attribute that apply to all spans generated by a process.
	// These should be used to represent underlying metadata about a process that’s non-ephemeral,
	// for example, the hostname of a process, or its instance ID.
	resources, err := resource.New(ctx,
		resource.WithFromEnv(), // pull attributes from OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME environment variables
		resource.WithAttributes(attribute.String("environment", os.Getenv(cas.Env_Env))),
	)
	if err != nil {
		return &OtlMetricService{}, err
	}

	var exporter sdk.Exporter
	if _, found := os.LookupEnv(common.Env_MetricsEndpoint); found {
		// The full endpoint URL will be taken from OTEL_EXPORTER_OTLP_METRICS_ENDPOINT. This allows testing locally
		// with an insecure endpoint and in actual infrastructure with a secure endpoint.
		exporter, err = otlpmetrichttp.New(ctx)
		if err != nil {
			return &OtlMetricService{}, err
		}
	} else {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		exporter, err = stdoutmetric.New(
			stdoutmetric.WithEncoder(enc),
		)
		if err != nil {
			return &OtlMetricService{}, err
		}
	}

	meterProvider := sdk.NewMeterProvider(
		sdk.WithResource(resources),
		sdk.WithReader(sdk.NewPeriodicReader(exporter)),
	)

	meter := meterProvider.Meter("go-cas")
	logger.Infof("started")

	return &OtlMetricService{meter: meter, meterProvider: meterProvider, logger: logger}, nil
}

func (o OtlMetricService) Shutdown(ctx context.Context) {
	o.meterProvider.Shutdown(ctx)
	o.logger.Infof("stopped")
}

func (o OtlMetricService) Count(ctx context.Context, name models.MetricName, val int) error {
	counter, err := o.meter.Int64Counter(string(name) + "_count")
	if err != nil {
		return err
	}
	counter.Add(ctx, int64(val))
	return nil
}

func (o OtlMetricService) Distribution(ctx context.Context, name models.MetricName, val int) error {
	histogram, err := o.meter.Int64Histogram(string(name) + "_histogram")
	if err != nil {
		return err
	}
	histogram.Record(ctx, int64(val))
	return nil
}

func (o OtlMetricService) QueueGauge(ctx context.Context, queueName string, monitor models.QueueMonitor) error {
	unprocessed, err := o.meter.Int64ObservableGauge(queueName + "_unprocessed_gauge")
	if err != nil {
		return err
	}
	inflight, err := o.meter.Int64ObservableGauge(queueName + "_inflight_gauge")
	if err != nil {
		return err
	}
	_, err = o.meter.RegisterCallback(
		func(_ context.Context, obs metric.Observer) error {
			if numUnprocessed, numInflight, err := monitor.GetUtilization(ctx); err != nil {
				return err
			} else {
				obs.ObserveInt64(unprocessed, int64(numUnprocessed))
				obs.ObserveInt64(inflight, int64(numInflight))
				return nil
			}
		},
		unprocessed,
		inflight,
	)
	return err
}
