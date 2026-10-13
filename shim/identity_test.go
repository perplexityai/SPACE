package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"go.opentelemetry.io/otel/attribute"
)

// identityEnvKeys is every input the resolver reads. Each case sets all of
// them so one case cannot leak identity into the next.
var identityEnvKeys = []string{
	"SPACE_SERVICE_NAME", "SPACE_ENV", "SPACE_VERSION",
	"OTEL_SERVICE_NAME", "OTEL_RESOURCE_ATTRIBUTES",
	"DD_SERVICE", "DD_ENV", "DD_VERSION",
}

func applyIdentityEnv(t *testing.T, settings map[string]string) {
	t.Helper()
	for _, key := range identityEnvKeys {
		t.Setenv(key, settings[key])
	}
	t.Setenv("SPACE_CLUSTER_ID", "")
	t.Setenv("HOSTNAME", "identity-host")
	t.Setenv("SPACE_K8S_NODE_NAME", "")
	t.Setenv("KUBE_NODE_NAME", "")
	t.Setenv("SPACE_K8S_POD_NAME", "")
	t.Setenv("SPACE_NODE_ID", "")
	// HOSTNAME only names a host outside Kubernetes, so the pinned value
	// cannot be asserted while the marker is present.
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	if err := os.Unsetenv("KUBERNETES_SERVICE_HOST"); err != nil {
		t.Fatal(err)
	}
}

func resourceAttribute(t *testing.T, attrs []attribute.KeyValue, key string) string {
	t.Helper()
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value.AsString()
		}
	}
	return ""
}

func TestTelemetryIdentityPrecedence(t *testing.T) {
	for _, test := range []struct {
		name        string
		settings    map[string]string
		service     string
		environment string
		version     string
		tags        []string
	}{
		{
			name: "SPACE only",
			settings: map[string]string{
				"SPACE_SERVICE_NAME": "space-service", "SPACE_ENV": "space-env", "SPACE_VERSION": "space-version",
			},
			service: "space-service", environment: "space-env", version: "space-version",
			tags: []string{"service:space-service", "version:space-version", "env:space-env"},
		},
		{
			name: "OTel service name only",
			settings: map[string]string{
				"OTEL_SERVICE_NAME": "otel-service",
			},
			service: "otel-service", environment: "", version: Version,
			tags: []string{"service:otel-service", fmt.Sprintf("version:%s", Version)},
		},
		{
			name: "OTel resource attributes only",
			settings: map[string]string{
				"OTEL_RESOURCE_ATTRIBUTES": "service.name=resource-service,deployment.environment.name=resource-env,service.version=resource-version,team=space",
			},
			service: "resource-service", environment: "resource-env", version: "resource-version",
			tags: []string{"service:resource-service", "version:resource-version", "env:resource-env"},
		},
		{
			// The Datadog fallback must stay raw so existing tags are preserved.
			name: "Datadog only",
			settings: map[string]string{
				"DD_SERVICE": "dd-service", "DD_ENV": "dd-env", "DD_VERSION": "dd-version",
			},
			service: "dd-service", environment: "dd-env", version: "dd-version",
			// The DogStatsD version tag keeps the build version; only the OTLP
			// resource takes DD_VERSION.
			tags: []string{"service:dd-service", fmt.Sprintf("version:%s", Version), "env:dd-env"},
		},
		{
			name: "all conflicting",
			settings: map[string]string{
				"SPACE_SERVICE_NAME": "space-service", "SPACE_ENV": "space-env", "SPACE_VERSION": "space-version",
				"OTEL_SERVICE_NAME":        "otel-service",
				"OTEL_RESOURCE_ATTRIBUTES": "service.name=resource-service,deployment.environment.name=resource-env,service.version=resource-version",
				"DD_SERVICE":               "dd-service", "DD_ENV": "dd-env", "DD_VERSION": "dd-version",
			},
			service: "space-service", environment: "space-env", version: "space-version",
			tags: []string{"service:space-service", "version:space-version", "env:space-env"},
		},
		{
			// A blank SPACE or OTel value is skipped; the Datadog value is not.
			name: "blank preferred settings fall through",
			settings: map[string]string{
				"SPACE_SERVICE_NAME": "  ", "OTEL_SERVICE_NAME": "\t",
				"OTEL_RESOURCE_ATTRIBUTES": "service.name= ,service.version=",
				"DD_SERVICE":               "dd-service", "DD_ENV": "dd-env", "DD_VERSION": "dd-version",
			},
			service: "dd-service", environment: "dd-env", version: "dd-version",
			// The DogStatsD version tag keeps the build version; only the OTLP
			// resource takes DD_VERSION.
			tags: []string{"service:dd-service", fmt.Sprintf("version:%s", Version), "env:dd-env"},
		},
		{
			name:     "nothing set",
			settings: nil,
			service:  defaultShimService, environment: "", version: Version,
			tags: []string{fmt.Sprintf("service:%s", defaultShimService), fmt.Sprintf("version:%s", Version)},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			applyIdentityEnv(t, test.settings)

			res, err := otlpResource(metricResourceAttributes()...)
			if err != nil {
				t.Fatal(err)
			}
			attrs := res.Attributes()
			if got := resourceAttribute(t, attrs, "service.name"); got != test.service {
				t.Errorf("resource service.name=%q, want %q", got, test.service)
			}
			if got := resourceAttribute(t, attrs, "deployment.environment.name"); got != test.environment {
				t.Errorf("resource deployment.environment.name=%q, want %q", got, test.environment)
			}
			if got := resourceAttribute(t, attrs, "service.version"); got != test.version {
				t.Errorf("resource service.version=%q, want %q", got, test.version)
			}

			if got := strings.Join(telemetryTags(), ","); got != strings.Join(test.tags, ",") {
				t.Errorf("DogStatsD tags=%q, want %q", got, test.tags)
			}

			// Producer identity is always local, never inherited.
			if got := resourceAttribute(t, attrs, "host.name"); got != "identity-host" {
				t.Errorf("resource host.name=%q, want the locally computed host", got)
			}
		})
	}
}

// mainTelemetryTags reproduces main's telemetryTags() exactly. A Datadog-only
// deployment's monitors read these series, so the resolver must not move them.
func mainTelemetryTags() []string {
	service := os.Getenv("DD_SERVICE")
	if service == "" {
		service = defaultShimService
	}
	tags := []string{
		fmt.Sprintf("service:%s", service),
		fmt.Sprintf("version:%s", Version),
	}
	if env := os.Getenv("DD_ENV"); env != "" {
		tags = append(tags, fmt.Sprintf("env:%s", env))
	}
	if clusterID := os.Getenv("SPACE_CLUSTER_ID"); clusterID != "" && isValidClusterID(clusterID) {
		tags = append(tags, fmt.Sprintf("cluster_id:%s", clusterID))
	}
	return tags
}

// mainTracerIdentity reproduces the service, env and version main passes to
// tracer.Start: gated on a nonblank value, passed raw.
func mainTracerIdentity() (string, string, string) {
	service := os.Getenv("DD_SERVICE")
	if service == "" {
		service = defaultShimService
	}
	var env, version string
	if raw := os.Getenv("DD_ENV"); strings.TrimSpace(raw) != "" {
		env = raw
	}
	if raw := os.Getenv("DD_VERSION"); strings.TrimSpace(raw) != "" {
		version = raw
	}
	return service, env, version
}

// With only DD_* set, the DogStatsD tags and the native tracer options must be
// byte-identical to main's, including padded and blank values and a
// DD_VERSION that differs from the build version.
func TestDatadogOnlyOutputMatchesMain(t *testing.T) {
	for _, settings := range []map[string]string{
		{},
		{"DD_SERVICE": "space-sandbox-manager"},
		{"DD_ENV": "prod"},
		{"DD_VERSION": "release-42"},
		{"DD_SERVICE": "svc", "DD_ENV": "prod", "DD_VERSION": "release-42"},
		// A DD_VERSION that differs from the build version must not reach the tag.
		{"DD_VERSION": "definitely-not-" + Version},
		// Padding is preserved by main, so it must be preserved here.
		{"DD_SERVICE": " svc ", "DD_ENV": " prod ", "DD_VERSION": " release-42 "},
		// Whitespace-only values: the tag keeps them, the tracer skips them.
		{"DD_SERVICE": " ", "DD_ENV": " ", "DD_VERSION": " "},
		{"DD_SERVICE": "", "DD_ENV": "", "DD_VERSION": ""},
		{"DD_SERVICE": "svc", "DD_ENV": "prod", "SPACE_CLUSTER_ID": "test-cluster"},
		// A malformed cluster id is dropped by both.
		{"DD_SERVICE": "svc", "SPACE_CLUSTER_ID": "Bad_Cluster"},
	} {
		t.Run(fmt.Sprint(settings), func(t *testing.T) {
			applyIdentityEnv(t, settings)
			if cluster, ok := settings["SPACE_CLUSTER_ID"]; ok {
				t.Setenv("SPACE_CLUSTER_ID", cluster)
			}

			if got, want := telemetryTags(), mainTelemetryTags(); !slices.Equal(got, want) {
				t.Errorf("DogStatsD tags=%q, want main's %q", got, want)
			}

			id := newIdentity(defaultShimService)
			wantService, wantEnv, wantVersion := mainTracerIdentity()
			if got := id.serviceName(); got != wantService {
				t.Errorf("tracer service=%q, want main's %q", got, wantService)
			}
			gotEnv := id.tracerEnvironment()
			if strings.TrimSpace(gotEnv) == "" {
				gotEnv = ""
			}
			if gotEnv != wantEnv {
				t.Errorf("tracer env=%q, want main's %q", gotEnv, wantEnv)
			}
			gotVersion := id.tracerVersion()
			if strings.TrimSpace(gotVersion) == "" {
				gotVersion = ""
			}
			if gotVersion != wantVersion {
				t.Errorf("tracer version=%q, want main's %q", gotVersion, wantVersion)
			}
		})
	}
}

// Resource attribute values are percent-decoded like the OTel SDK, matching
// the Rust resolver, so one shared setting means the same thing to both.
func TestResourceAttributeValuesArePercentDecoded(t *testing.T) {
	for _, test := range []struct{ raw, service, environment string }{
		{"service.name=service%2Fapi", "service/api", ""},
		{"service.name=a%20b", "a b", ""},
		{"service.name=100%25", "100%", ""},
		{"service.name=svc,deployment.environment.name=eu%2Dwest", "svc", "eu-west"},
		// Malformed escapes are kept verbatim.
		{"service.name=50%", "50%", ""},
		{"service.name=%zz", "%zz", ""},
		{"service.name=%2", "%2", ""},
		{"service.name=a%2", "a%2", ""},
		// A decode that is not valid UTF-8 cannot be honored.
		{"service.name=%ff", "%ff", ""},
	} {
		t.Run(test.raw, func(t *testing.T) {
			applyIdentityEnv(t, map[string]string{"OTEL_RESOURCE_ATTRIBUTES": test.raw})
			id := newIdentity(defaultShimService)
			if got := id.serviceName(); got != test.service {
				t.Errorf("service=%q, want %q", got, test.service)
			}
			if got := id.resourceEnvironment(); got != test.environment {
				t.Errorf("environment=%q, want %q", got, test.environment)
			}
		})
	}
}

// dogstatsdDatagrams emits one counter through a DogStatsD sink built with the
// given constant tags and returns the bytes that reached the socket.
func dogstatsdDatagrams(t *testing.T, tags []string) string {
	t.Helper()
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := statsd.New(
		fmt.Sprintf("127.0.0.1:%d", listener.LocalAddr().(*net.UDPAddr).Port),
		statsd.WithTags(tags),
	)
	if err != nil {
		t.Fatal(err)
	}
	sink := &datadogMetricsSink{client}
	if err := sink.count("space.shim.operation.error", 3, []string{"op:create"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownBudget)
	defer cancel()
	if err := sink.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := listener.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, 8192)
	n, err := listener.Read(packet)
	if err != nil {
		t.Fatalf("no datagram arrived: %v", err)
	}
	datagram := string(packet[:n])
	// Guard the comparison itself: two tagless datagrams would match trivially.
	if !strings.Contains(datagram, "|#") {
		t.Fatalf("datagram carries no tag section: %q", datagram)
	}
	return datagram
}

// The tag list is what a monitor reads, but the datagram is what the Agent
// receives. Compare the wire bytes so tag order, separators and escaping are
// covered too, not only the slice this shim computes.
func TestDatadogOnlyDatagramsMatchMain(t *testing.T) {
	for _, settings := range []map[string]string{
		{},
		{"DD_SERVICE": "space-sandbox-manager"},
		{"DD_SERVICE": "svc", "DD_ENV": "prod", "DD_VERSION": "release-42"},
		{"DD_VERSION": "definitely-not-" + Version},
		{"DD_SERVICE": " svc ", "DD_ENV": " prod ", "DD_VERSION": " release-42 "},
		{"DD_SERVICE": " ", "DD_ENV": " ", "DD_VERSION": " "},
		{"DD_SERVICE": "svc", "DD_ENV": "prod", "SPACE_CLUSTER_ID": "test-cluster"},
	} {
		t.Run(fmt.Sprint(settings), func(t *testing.T) {
			applyIdentityEnv(t, settings)
			if cluster, ok := settings["SPACE_CLUSTER_ID"]; ok {
				t.Setenv("SPACE_CLUSTER_ID", cluster)
			}
			got := dogstatsdDatagrams(t, telemetryTags())
			want := dogstatsdDatagrams(t, mainTelemetryTags())
			if got != want {
				t.Errorf("datagram=%q, want main's %q", got, want)
			}
		})
	}
}

// captureShimLog collects what logShim writes while fn runs. logShim writes to
// os.Stderr directly, so the file is swapped for a pipe rather than threading
// a writer through production code that has no other use for one.
func captureShimLog(t *testing.T, fn func()) string {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stderr
	defer func() { os.Stderr = saved }()
	os.Stderr = writer
	fn()
	os.Stderr = saved
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	captured, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	return string(captured)
}

// An OTLP-only deployment must carry the resolved identity and nothing else:
// no DogStatsD datagram leaks to the Agent, and no DD_* tag rides along.
func TestOTLPOnlyCarriesOnlyTheResolvedIdentity(t *testing.T) {
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()

	receiver, endpoint := startMetricsReceiver(t)
	applyIdentityEnv(t, map[string]string{
		"SPACE_SERVICE_NAME": "space-service",
		"SPACE_ENV":          "space-env",
		"SPACE_VERSION":      "space-version",
		"DD_SERVICE":         "dd-service",
		"DD_ENV":             "dd-env",
		"DD_VERSION":         "dd-version",
	})
	t.Setenv("SPACE_METRICS_SINK", "otlp")
	t.Setenv("SPACE_TRACE_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", endpoint)
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "grpc")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "")
	t.Setenv("DD_DOGSTATSD_SOCKET", "")
	t.Setenv("DD_AGENT_HOST", "127.0.0.1")
	t.Setenv("DD_DOGSTATSD_PORT", strconv.Itoa(udp.LocalAddr().(*net.UDPAddr).Port))

	sink, err := newMetricsSink()
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.count("space.shim.operation.error", 1, []string{"op:create"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownBudget)
	defer cancel()
	if err := sink.shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	// The Agent socket stays silent: nothing selected DogStatsD.
	if err := udp.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, 2048)
	if n, err := udp.Read(packet); err == nil {
		t.Errorf("OTLP-only run sent DogStatsD traffic: %q", packet[:n])
	}

	request := <-receiver.requests
	attrs := request.ResourceMetrics[0].Resource.Attributes
	assertMetricAttributes(t, attrs, map[string]string{
		"service.name":                "space-service",
		"deployment.environment.name": "space-env",
		"service.version":             "space-version",
	})
	// No DD_* value rides along on the resource or on the points.
	for _, attr := range attrs {
		if strings.HasPrefix(attr.Value.GetStringValue(), "dd-") {
			t.Errorf("OTLP resource carried a Datadog value: %v", attr)
		}
	}
}

// The prefix logShim writes, so a warning can be counted rather than lines.
const shimLogPrefix = "[space-shim] "

// A mixed identity is reported once per process; a single-path deployment
// never sees it.
//
// Each row runs the bootstrap check twice inside one capture. The warning is
// emitted from there rather than from the lazily built sink, and calling it
// twice is what proves "once" holds for a process bootstrapping more than one
// entry point.
func TestMixedIdentityWarning(t *testing.T) {
	for _, test := range []struct {
		name     string
		settings map[string]string
		// Dimensions the warning must name. Empty means it must stay silent:
		// there is nothing for an operator to reconcile.
		wantFields []string
		// Mixed only: the tags the resolver wins, and the Datadog ones it must
		// not attach but which the client still puts on the wire beside them.
		spaceTags   []string
		datadogTags []string
	}{
		{
			name: "mixed",
			settings: map[string]string{
				"DD_SERVICE": "dd-service", "DD_ENV": "dd-env", "DD_VERSION": "dd-version",
				"SPACE_SERVICE_NAME": "space-service", "SPACE_ENV": "space-env",
				"SPACE_VERSION": "space-version",
			},
			wantFields:  []string{"service", "env", "version"},
			spaceTags:   []string{"service:space-service", "env:space-env", "version:space-version"},
			datadogTags: []string{"service:dd-service", "env:dd-env", "version:dd-version"},
		},
		{
			// The client injects a DD_* value untrimmed, so a padded one is a
			// different tag value rather than the same one.
			name:       "padded",
			settings:   map[string]string{"SPACE_ENV": "prod", "DD_ENV": " prod "},
			wantFields: []string{"env"},
		},
		{name: "datadog only", settings: map[string]string{"DD_SERVICE": "svc", "DD_ENV": "prod", "DD_VERSION": "r42"}},
		{name: "space only", settings: map[string]string{"SPACE_SERVICE_NAME": "svc", "SPACE_ENV": "prod", "SPACE_VERSION": "r42"}},
		{name: "otel only", settings: map[string]string{"OTEL_SERVICE_NAME": "svc"}},
		{name: "agreeing", settings: map[string]string{"DD_SERVICE": "svc", "SPACE_SERVICE_NAME": "svc"}},
		// An empty DD_* value is no tag at all, so nothing conflicts.
		{name: "empty datadog", settings: map[string]string{"SPACE_SERVICE_NAME": "svc", "DD_SERVICE": ""}},
	} {
		t.Run(test.name, func(t *testing.T) {
			applyIdentityEnv(t, test.settings)
			stderr := captureShimLog(t, func() {
				mixedIdentityWarning = sync.Once{}
				for range 2 {
					if err := validateMetricsConfiguration(); err != nil {
						t.Fatal(err)
					}
				}
			})

			if len(test.wantFields) == 0 {
				if stderr != "" {
					t.Fatalf("%s must not warn, got %q", test.name, stderr)
				}
				return
			}
			if count := strings.Count(stderr, shimLogPrefix); count != 1 {
				t.Errorf("want exactly one warning, got %d: %q", count, stderr)
			}
			for _, want := range append(test.wantFields, "DD_SERVICE", "DD_ENV", "DD_VERSION") {
				if !strings.Contains(stderr, want) {
					t.Errorf("warning must name %q, got %q", want, stderr)
				}
			}
			// It names the settings, never their values.
			for _, value := range test.settings {
				if trimmed := strings.TrimSpace(value); trimmed != "" && strings.Contains(stderr, trimmed) {
					t.Errorf("warning leaked %q: %q", trimmed, stderr)
				}
			}

			if test.spaceTags == nil {
				return
			}
			// Both identities reach the wire: the resolver decides what this
			// shim attaches, and the Datadog client adds its own from DD_*
			// regardless. That is the limitation the warning names.
			tags := telemetryTags()
			datagram := dogstatsdDatagrams(t, tags)
			for _, tag := range append(test.spaceTags, test.datadogTags...) {
				fromSpace := slices.Contains(test.spaceTags, tag)
				if slices.Contains(tags, tag) != fromSpace {
					t.Errorf("tags=%q: want %q attached=%v", tags, tag, fromSpace)
				}
				if !strings.Contains(datagram, tag) {
					t.Errorf("datagram=%q, want %q", datagram, tag)
				}
			}
		})
	}
}
