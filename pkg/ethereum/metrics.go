package ethereum

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	nodesTotal *prometheus.GaugeVec
}

var (
	metricsInstance *Metrics
	once            sync.Once
)

func GetMetricsInstance(namespace string) *Metrics {
	once.Do(func() {
		metricsInstance = &Metrics{
			nodesTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "nodes_total",
				Help:      "Total number of nodes in the pool",
			}, []string{"type", "status"}),
		}

		prometheus.MustRegister(metricsInstance.nodesTotal)
	})

	return metricsInstance
}

func (m *Metrics) SetNodesTotal(count float64, labels []string) {
	if m == nil || m.nodesTotal == nil {
		return
	}

	m.nodesTotal.WithLabelValues(labels...).Set(count)
}
