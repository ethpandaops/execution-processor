package cryo

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// CryoBuildInfo exposes which cryo produced the data, which is the first
	// thing you need when the output looks wrong.
	CryoBuildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "execution_processor_cryo_build_info",
		Help: "Version of the cryo binary the processor invokes",
	}, []string{"network", "version"})

	// CryoFetchDuration measures one cryo invocation.
	CryoFetchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "execution_processor_cryo_fetch_duration_seconds",
		Help:    "Time taken by one cryo invocation, covering all datasets in a group",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
	}, []string{"network", "processor"})

	// CryoDecodeDuration measures reading one group's parquet output into memory.
	CryoDecodeDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "execution_processor_cryo_decode_duration_seconds",
		Help:    "Time taken to decode one group's parquet output",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"network", "processor"})

	// CryoSubmitDuration measures mapping rows and waiting for every dataset's
	// buffer to flush, which is where ClickHouse backpressure shows up.
	CryoSubmitDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "execution_processor_cryo_submit_duration_seconds",
		Help:    "Time taken to map and flush one group's rows to ClickHouse",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	}, []string{"network", "processor"})

	// CryoRowsDecoded counts rows produced per dataset.
	CryoRowsDecoded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_cryo_rows_decoded_total",
		Help: "Rows decoded from cryo output",
	}, []string{"network", "processor", "dataset"})

	// CryoFetchErrors counts failed invocations by cause.
	CryoFetchErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_cryo_fetch_errors_total",
		Help: "Failed cryo invocations",
	}, []string{"network", "processor", "reason"})
)
