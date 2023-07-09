package metrics

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/metric"
	sdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/ceramicnetwork/go-cas/models"
)

type OtlMetricService struct {
	meterProvider *sdk.MeterProvider
	meter         metric.Meter
}

func NewMetricService(ctx context.Context, collectorHost string) (models.MetricService, error) {
	var resources *resource.Resource
	// Resources are a special type of attribute that apply to all spans generated by a process.
	// These should be used to represent underlying metadata about a process that’s non-ephemeral,
	// for example, the hostname of a process, or its instance ID.
	resources, err := resource.New(ctx,
		resource.WithFromEnv(), // pull attributes from OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME environment variables
		resource.WithAttributes(attribute.String("environment", os.Getenv("ENV"))),
	)
	if err != nil {
		return &OtlMetricService{}, err
	}

	var exporter sdk.Exporter
	if collectorHost != "" {
		options := otlpmetrichttp.WithEndpoint("%s:4318")
		exporter, err = otlpmetrichttp.New(ctx, options)
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
	log.Printf("metrics: started")

	return &OtlMetricService{meter: meter, meterProvider: meterProvider}, nil
}

func (o OtlMetricService) Shutdown(ctx context.Context) {
	o.meterProvider.Shutdown(ctx)
	log.Printf("metrics: stopped")
}

func (o OtlMetricService) Count(ctx context.Context, name models.MetricName, val int) error {
	counter, err := o.meter.Int64Counter(string(name) + "_count")
	if err != nil {
		return err
	}

	counter.Add(ctx, int64(val))

	return nil
}
