package collector

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/oliveagle/jsonpath"
	autoscalingv2 "k8s.io/api/autoscaling/v2beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/metrics/pkg/apis/external_metrics"
)

const (
	JSONMetricName          = "json-query"
)

type JSONCollectorPlugin struct {
	client  kubernetes.Interface
}

type JSONCollector struct {
	client  kubernetes.Interface
	getter  *JSONPathMetricsGetter

	metric          autoscalingv2.MetricIdentifier
	interval        time.Duration
	hpa             *autoscalingv2.HorizontalPodAutoscaler
}

// JSONPathMetricsGetter is a metrics getter which looks up pod metrics by
// querying the pods metrics endpoint and lookup the metric value as defined by
// the json path query.
type JSONPathMetricsGetter struct {
	jsonPath   *jsonpath.Compiled
	scheme     string
	host       string
	path       string
	port       int
	aggregator string
}

func NewJSONCollectorPlugin(client kubernetes.Interface) (*JSONCollectorPlugin, error) {
	return &JSONCollectorPlugin{
		client:  client,
	}, nil
}

func (p *JSONCollectorPlugin) NewCollector(hpa *autoscalingv2.HorizontalPodAutoscaler, config *MetricConfig, interval time.Duration) (Collector, error) {
	return NewJSONCollector(p.client, hpa, config, interval)
}


func NewJSONCollector(client kubernetes.Interface, hpa *autoscalingv2.HorizontalPodAutoscaler, config *MetricConfig, interval time.Duration) (*JSONCollector, error) {
	getter, err := NewJSONPathMetricsGetter(config.Config)
	if err != nil {
		return nil, err
	}

	if config.Metric.Selector == nil {
		return nil, fmt.Errorf("selector for queue is not specified")
	}

	tmp := make(map[string]string)
	for k, v := range config.Metric.Selector.MatchLabels {
		if k != "json-key" && k != "port" && k != "path" && k!= "scheme" && k!= "host" {
			tmp[k] = v
		}
	}
	config.Metric.Selector.MatchLabels = tmp
	log.Printf("XXXY MatchLabels=%+v\n", config.Metric.Selector.MatchLabels)

	if config.Type != autoscalingv2.ExternalMetricSourceType {
		return nil, fmt.Errorf("Only external metric type is supported")
	}

	c := &JSONCollector{
		client:     client,
		interval:   interval,
		metric:     config.Metric,
		hpa:        hpa,
		getter:     getter,
	}

	log.Printf("XXXJ config=%+v\n", config.Config)

	return c, nil
}

// NewJSONPathMetricsGetter initializes a new JSONPathMetricsGetter.
func NewJSONPathMetricsGetter(config map[string]string) (*JSONPathMetricsGetter, error) {
	getter := &JSONPathMetricsGetter{}

	if v, ok := config["json-key"]; ok {
		path, err := jsonpath.Compile(v)
		if err != nil {
			return nil, fmt.Errorf("failed to parse json path definition: %v", err)
		}

		getter.jsonPath = path
	}

	if v, ok := config["scheme"]; ok {
		getter.scheme = v
	}

	if v, ok := config["host"]; ok {
		getter.host = v
	}

	if v, ok := config["path"]; ok {
		getter.path = v
	}

	if v, ok := config["port"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}
		getter.port = n
	}

	if v, ok := config["aggregator"]; ok {
		getter.aggregator = v
	}

	return getter, nil
}

// GetPodMetric gets metric from pod by fetching json metrics from the pods metric
// endpoint and extracting the desired value using the specified json path
// query.
func (g *JSONPathMetricsGetter) GetPodMetric(pod *corev1.Pod) (float64, error) {
	if pod.Status.PodIP == "" {
		return 0, fmt.Errorf("pod %s/%s does not have a pod IP", pod.Namespace, pod.Namespace)
	}

	data, err := getJsonMetrics(g.scheme, pod.Status.PodIP, g.path, g.port)
	if err != nil {
		return 0, err
	}

	return ExtractJSONMetric(g, data);
}

func ExtractJSONMetric(g *JSONPathMetricsGetter, data []byte) (float64, error) {
	// parse data
	var jsonData interface{}
	err := json.Unmarshal(data, &jsonData)
	if err != nil {
		return 0, err
	}

	res, err := g.jsonPath.Lookup(jsonData)
	if err != nil {
		return 0, err
	}

	switch res := res.(type) {
	case int:
		return float64(res), nil
	case float32:
		return float64(res), nil
	case float64:
		return res, nil
	case []int:
		return reduce(intsToFloat64s(res), g.aggregator)
	case []float32:
		return reduce(float32sToFloat64s(res), g.aggregator)
	case []float64:
		return reduce(res, g.aggregator)
	default:
		return 0, fmt.Errorf("unsupported type %T", res)
	}
}

func (c *JSONCollector) GetMetrics() ([]CollectedMetric, error) {

	log.Printf("XXX M scheme=%+v host=%+v path=%+v port=%+v\n", c.getter.scheme, c.getter.host, c.getter.path, c.getter.port)
	data, err := getJsonMetrics(c.getter.scheme, c.getter.host, c.getter.path, c.getter.port)
	if err != nil {
		log.Printf("XXXE1")
		return nil, err
	}

	sampleValue, err :=  ExtractJSONMetric(c.getter, data);
	if err != nil {
		log.Printf("XXXE2")
		return nil, err
	}

	/*if c.perReplica {
		// get current replicas for the targeted scale object. This is used to
		// calculate an average metric instead of total.
		// targetAverageValue will be available in Kubernetes v1.12
		// https://github.com/kubernetes/kubernetes/pull/64097
		replicas, err := targetRefReplicas(c.client, c.hpa)
		if err != nil {
			return nil, err
		}
		sampleValue = model.SampleValue(float64(sampleValue) / float64(replicas))
	}*/

	log.Printf("XXXE0")
	var metricValue CollectedMetric

	metricValue = CollectedMetric{
		Type: autoscalingv2.ExternalMetricSourceType,
		External: external_metrics.ExternalMetricValue{
			MetricName:   c.metric.Name,
			MetricLabels: c.metric.Selector.MatchLabels,
			Timestamp:    metav1.Time{Time: time.Now().UTC()},
			Value:        *resource.NewMilliQuantity(int64(sampleValue*1000), resource.DecimalSI),
		},
	}

	return []CollectedMetric{metricValue}, nil
}

func (c *JSONCollector) Interval() time.Duration {
	return c.interval
}

// getPodMetrics returns the content of the host metrics endpoint.
func getJsonMetrics(scheme, host, path string, port int) ([]byte, error) {
	httpClient := &http.Client{
		Timeout:   15 * time.Second,
		Transport: &http.Transport{},
	}

	if scheme == "" {
		scheme = "http"
	}

	metricsURL := url.URL{
		Scheme: scheme,
		Host:   fmt.Sprintf("%s:%d", host, port),
		Path:   path,
	}

	request, err := http.NewRequest(http.MethodGet, metricsURL.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unsuccessful response: %s", resp.Status)
	}

	return data, nil
}

// intsToFloat64s will convert a slice of int to a slice of float64
func intsToFloat64s(in []int) (out []float64) {
	out = []float64{}
	for _, v := range in {
		out = append(out, float64(v))
	}
	return
}

// float32sToFloat64s will convert a slice of float32 to a slice of float64
func float32sToFloat64s(in []float32) (out []float64) {
	out = []float64{}
	for _, v := range in {
		out = append(out, float64(v))
	}
	return
}

// reduce will reduce a slice of numbers given a aggregator function's name. If it's empty or not recognized, an error is returned.
func reduce(values []float64, aggregator string) (float64, error) {
	switch aggregator {
	case "avg":
		return avg(values), nil
	case "min":
		return min(values), nil
	case "max":
		return max(values), nil
	case "sum":
		return sum(values), nil
	default:
		return 0, fmt.Errorf("slice of numbers was returned by JSONPath, but no valid aggregator function was specified: %v", aggregator)
	}
}

// avg implements the average mathematical function over a slice of float64
func avg(values []float64) float64 {
	sum := sum(values)
	return sum / float64(len(values))
}

// min implements the absolute minimum mathematical function over a slice of float64
func min(values []float64) float64 {
	// initialized with positive infinity, all finite numbers are smaller than it
	curMin := math.Inf(1)

	for _, v := range values {
		if v < curMin {
			curMin = v
		}
	}

	return curMin
}

// max implements the absolute maximum mathematical function over a slice of float64
func max(values []float64) float64 {
	// initialized with negative infinity, all finite numbers are bigger than it
	curMax := math.Inf(-1)

	for _, v := range values {
		if v > curMax {
			curMax = v
		}
	}

	return curMax
}

// sum implements the summation mathematical function over a slice of float64
func sum(values []float64) float64 {
	res := 0.0

	for _, v := range values {
		res += v
	}

	return res
}
