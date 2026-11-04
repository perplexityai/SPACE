package main

import (
	"os"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Telemetry identity resolves as SPACE > OTel > DD > default, matching
// crates/l10_foundation/observability/src/identity.rs so a shim and the Rust services
// beside it agree on who they are.
//
// A blank SPACE or OTel value is skipped, so the first nonblank one wins. The
// Datadog fallback is used raw: existing DogStatsD tags include empty and
// whitespace values, and normalizing them here would rewrite the tags a
// Datadog-only deployment already emits.
type identity struct {
	defaultService string
	service        setting
	environment    setting
	version        setting
	// Shared resource attributes that carry no identity of their own.
	attributes map[string]string
}

type setting struct {
	preferred string // SPACE or OTel, already reduced to the first nonblank
	datadog   string // raw DD_* value
}

func (s setting) raw() string {
	if s.preferred != "" {
		return s.preferred
	}
	return s.datadog
}

// nonblank reports the resolved value only when it carries content, for the
// OTLP resource, which must not publish an empty or whitespace attribute.
func (s setting) nonblank() string {
	if value := strings.TrimSpace(s.raw()); value != "" {
		return value
	}
	return ""
}

// producerAttributeKeys identify the process, node or pod. They are always
// computed locally and never inherited, so one shared OTEL_RESOURCE_ATTRIBUTES
// cannot make two processes look like one.
var producerAttributeKeys = []string{
	"service.instance.id",
	"host.name",
	"k8s.node.name",
	"k8s.pod.name",
}

func newIdentity(defaultService string) identity {
	return identityFromLookup(defaultService, os.Getenv)
}

func identityFromLookup(defaultService string, lookup func(string) string) identity {
	attributes := parseResourceAttributes(lookup("OTEL_RESOURCE_ATTRIBUTES"))
	take := func(key string) string {
		value := attributes[key]
		delete(attributes, key)
		return value
	}
	for _, key := range producerAttributeKeys {
		delete(attributes, key)
	}
	return identity{
		defaultService: defaultService,
		service: setting{
			preferred: firstNonblank(lookup("SPACE_SERVICE_NAME"), lookup("OTEL_SERVICE_NAME"), take("service.name")),
			datadog:   lookup("DD_SERVICE"),
		},
		environment: setting{
			preferred: firstNonblank(lookup("SPACE_ENV"), take("deployment.environment.name")),
			datadog:   lookup("DD_ENV"),
		},
		version: setting{
			preferred: firstNonblank(lookup("SPACE_VERSION"), take("service.version")),
			datadog:   lookup("DD_VERSION"),
		},
		attributes: attributes,
	}
}

// Match the SDK's comma-separated key=value grammar, including values that
// contain '='. Malformed entries are ignored without logging their content.
func parseResourceAttributes(raw string) map[string]string {
	attributes := make(map[string]string)
	for _, entry := range strings.Split(raw, ",") {
		if key, value, found := strings.Cut(entry, "="); found {
			attributes[strings.TrimSpace(key)] = percentDecode(strings.TrimSpace(value))
		}
	}
	return attributes
}

// percentDecode decodes a resource attribute value the way the OTel SDK does,
// keeping a malformed escape verbatim, so one shared
// OTEL_RESOURCE_ATTRIBUTES means the same thing to every SPACE producer.
func percentDecode(value string) string {
	if !strings.Contains(value, "%") {
		return value
	}
	decoded := make([]byte, 0, len(value))
	for rest := value; rest != ""; {
		if strings.HasPrefix(rest, "%") && len(rest) >= 3 {
			if byteValue, err := strconv.ParseUint(rest[1:3], 16, 8); err == nil {
				decoded = append(decoded, byte(byteValue))
				rest = rest[3:]
				continue
			}
		}
		decoded = append(decoded, rest[0])
		rest = rest[1:]
	}
	// A decode that is not valid UTF-8 was not an escape this producer can
	// honor; keep the original so every producer still agrees on it.
	if !utf8.Valid(decoded) {
		return value
	}
	return string(decoded)
}

func firstNonblank(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// serviceName is the one name this process reports, on the OTLP resource and
// on every metric point alike.
func (i identity) serviceName() string {
	if value := i.service.raw(); value != "" {
		return value
	}
	return i.defaultService
}

func (i identity) metricEnvironment() string { return i.environment.raw() }

// metricVersion is the DogStatsD version tag. It deliberately omits the
// DD_VERSION fallback: that tag has always carried the shim's build version,
// so including it would rewrite the tags a Datadog-only deployment emits and
// move the series its monitors read. An explicit SPACE or OTel version still
// governs it.
func (i identity) metricVersion() string {
	if i.version.preferred != "" {
		return i.version.preferred
	}
	return Version
}

// resourceVersion is the OTLP resource's service.version, where DD_VERSION is
// a legitimate fallback because no existing series depends on it.
func (i identity) resourceVersion() string {
	if value := i.version.nonblank(); value != "" {
		return value
	}
	return Version
}

func (i identity) resourceEnvironment() string { return i.environment.nonblank() }

// tracerEnvironment and tracerVersion feed the native Datadog tracer, which
// has always received the raw DD_* value. Trimming here would change the
// options a Datadog-only deployment starts with.
func (i identity) tracerEnvironment() string { return i.environment.raw() }

func (i identity) tracerVersion() string { return i.version.raw() }
