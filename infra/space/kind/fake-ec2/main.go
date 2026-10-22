// Command space-kind-fake-ec2 is the Kind-only EC2 endpoint the
// cluster-controller's reconcile-queue GC calls through
// AWS_ENDPOINT_URL_EC2 (infra/space/kind/charts/space-kind-fake-ec2). The
// GC's Ec2Detector asks DescribeInstances, filtered on private-dns-name, for
// the instance behind a non-ready node's Kubernetes name; on EKS that name is
// the instance's private DNS name. This fixture answers that one call from
// the cluster's Node objects, which stand in for the instances behind them:
//
//   - a Node whose Ready condition is True is a running instance;
//   - any other Node (NotReady, Unknown) is a stopped one;
//   - no Node is no instance, as EC2 answers once a terminated instance has
//     aged out of DescribeInstances;
//   - a Kubernetes API error is an EC2 error (Unavailable, HTTP 503).
//
// It checks that the request is signed for EC2 in the cluster's region, but
// not the signature itself, and refuses every other action and filter, so a
// call the GC starts making shows up as an error rather than a made-up
// answer. Each request is logged, which is the record that the GC's calls
// stayed in the cluster.
//
// It is a test fixture, like space-kind-sts-bridge: it stands in for EC2, and
// is never deployed outside a local cluster.
package main

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

const (
	ec2NS          = "http://ec2.amazonaws.com/doc/2016-11-15/"
	ec2Version     = "2016-11-15"
	serviceAccount = "/var/run/secrets/kubernetes.io/serviceaccount"
)

// nodeName is a DNS-1123 subdomain, the form of every Node name.
var nodeName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$`)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("space-kind-fake-ec2", flag.ExitOnError)
	listen := fs.String("listen", ":8443", "listen address (HTTPS), the cluster-controller's AWS_ENDPOINT_URL_EC2")
	cert := fs.String("tls-cert", "", "listener certificate chain, leaf first")
	key := fs.String("tls-key", "", "listener key")
	region := fs.String("region", "", "the region requests must be signed for (the cluster-controller's AWS_REGION)")
	_ = fs.Parse(args)
	for _, f := range []struct{ name, value string }{
		{"-tls-cert", *cert}, {"-tls-key", *key}, {"-region", *region},
	} {
		if f.value == "" {
			return fmt.Errorf("%s is required", f.name)
		}
	}
	nodes, err := inClusterNodes()
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.Handle("POST /", &fakeEC2{region: *region, nodes: nodes, log: slog.New(slog.NewJSONHandler(os.Stdout, nil))})
	srv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    16 << 10,
	}
	log.Printf("ec2 on %s for region %s", *listen, *region)
	return srv.ListenAndServeTLS(*cert, *key)
}

// node is the part of a Kubernetes Node the answer depends on.
type node struct {
	Metadata struct {
		UID string `json:"uid"`
	} `json:"metadata"`
	Status struct {
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
	} `json:"status"`
}

// nodeGetter reads one Node: nil and no error when it does not exist.
type nodeGetter func(name string) (*node, error)

// inClusterNodes reads Nodes from the API server with the pod's ServiceAccount.
func inClusterNodes() (nodeGetter, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("not running in a cluster: KUBERNETES_SERVICE_HOST/PORT are unset")
	}
	ca, err := os.ReadFile(serviceAccount + "/ca.crt")
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("the ServiceAccount CA holds no PEM certificate")
	}
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}},
	}
	base := "https://" + net.JoinHostPort(host, port) + "/api/v1/nodes/"
	return func(name string) (*node, error) {
		// Re-read per request: the kubelet rotates the projected token.
		token, err := os.ReadFile(serviceAccount + "/token")
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequest(http.MethodGet, base+url.PathEscape(name), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusOK:
			var n node
			if err := json.NewDecoder(resp.Body).Decode(&n); err != nil {
				return nil, err
			}
			return &n, nil
		case http.StatusNotFound:
			return nil, nil
		default:
			return nil, fmt.Errorf("GET node %s: HTTP %d", name, resp.StatusCode)
		}
	}, nil
}

type fakeEC2 struct {
	region string
	nodes  nodeGetter
	log    *slog.Logger
}

type instanceState struct {
	Code int    `xml:"code"`
	Name string `xml:"name"`
}

type instance struct {
	InstanceID     string        `xml:"instanceId"`
	InstanceState  instanceState `xml:"instanceState"`
	PrivateDNSName string        `xml:"privateDnsName"`
}

type reservation struct {
	ReservationID string     `xml:"reservationId"`
	Instances     []instance `xml:"instancesSet>item"`
}

type describeInstancesResponse struct {
	XMLName      xml.Name      `xml:"DescribeInstancesResponse"`
	Xmlns        string        `xml:"xmlns,attr"`
	RequestID    string        `xml:"requestId"`
	Reservations []reservation `xml:"reservationSet>item"`
}

type errorResponse struct {
	XMLName   xml.Name `xml:"Response"`
	Code      string   `xml:"Errors>Error>Code"`
	Message   string   `xml:"Errors>Error>Message"`
	RequestID string   `xml:"RequestID"`
}

func (f *fakeEC2) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := newRequestID()
	entry := []any{"request_id", reqID, "remote", r.RemoteAddr}
	fail := func(status int, code, message string) {
		f.log.Warn("refused", append(entry, "status", status, "code", code, "message", message)...)
		writeXML(w, status, errorResponse{Code: code, Message: message, RequestID: reqID})
	}

	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	if err := r.ParseForm(); err != nil {
		fail(http.StatusBadRequest, "InvalidParameterValue", "unreadable form body: "+err.Error())
		return
	}
	if scope := credentialScope(r.Header.Get("Authorization")); scope != f.region+"/ec2/aws4_request" {
		fail(http.StatusUnauthorized, "AuthFailure", fmt.Sprintf("request is not signed for ec2 in %s (scope %q)", f.region, scope))
		return
	}
	form := r.PostForm
	entry = append(entry, "action", form.Get("Action"))
	if form.Get("Action") != "DescribeInstances" || form.Get("Version") != ec2Version {
		fail(http.StatusBadRequest, "InvalidAction", "this fake answers only DescribeInstances, version "+ec2Version)
		return
	}
	name := form.Get("Filter.1.Value.1")
	for param := range form {
		switch param {
		case "Action", "Version", "Filter.1.Name", "Filter.1.Value.1", "MaxResults", "NextToken":
		default:
			fail(http.StatusBadRequest, "InvalidParameterValue", "unsupported parameter "+param)
			return
		}
	}
	if form.Get("Filter.1.Name") != "private-dns-name" || !nodeName.MatchString(name) {
		fail(http.StatusBadRequest, "InvalidParameterValue", "this fake answers only a filter on one private-dns-name")
		return
	}
	entry = append(entry, "private_dns_name", name)

	n, err := f.nodes(name)
	if err != nil {
		fail(http.StatusServiceUnavailable, "Unavailable", "the Kubernetes API did not answer: "+err.Error())
		return
	}
	resp := describeInstancesResponse{Xmlns: ec2NS, RequestID: reqID}
	if n == nil {
		f.log.Info("answered", append(entry, "status", http.StatusOK, "answer", "no instance")...)
	} else {
		inst := instance{InstanceID: instanceID(n.Metadata.UID), InstanceState: state(n), PrivateDNSName: name}
		resp.Reservations = []reservation{{ReservationID: "r-" + inst.InstanceID[2:], Instances: []instance{inst}}}
		f.log.Info("answered", append(entry, "status", http.StatusOK, "answer", inst.InstanceState.Name,
			"instance_id", inst.InstanceID)...)
	}
	writeXML(w, http.StatusOK, resp)
}

// state maps the Node's Ready condition onto an EC2 instance state.
func state(n *node) instanceState {
	for _, c := range n.Status.Conditions {
		if c.Type == "Ready" && c.Status == "True" {
			return instanceState{Code: 16, Name: "running"}
		}
	}
	return instanceState{Code: 80, Name: "stopped"}
}

// instanceID derives a stable, EC2-shaped ID from the Node's UID, so a Node
// re-registered under the same name reads as a new instance, as it would be.
func instanceID(uid string) string {
	sum := sha256.Sum256([]byte(uid))
	return "i-" + hex.EncodeToString(sum[:])[:17]
}

// credentialScope returns "<region>/<service>/aws4_request" from a SigV4
// Authorization header, or "" when there is none.
func credentialScope(authorization string) string {
	_, rest, ok := strings.Cut(authorization, "Credential=")
	if !ok {
		return ""
	}
	credential, _, _ := strings.Cut(rest, ",")
	// AccessKey/Date/Region/Service/aws4_request
	parts := strings.Split(credential, "/")
	if len(parts) != 5 {
		return ""
	}
	return strings.Join(parts[2:], "/")
}

func newRequestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

func writeXML(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "text/xml;charset=UTF-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(body)
}
