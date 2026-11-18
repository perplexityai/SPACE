// Command space-kind-fake-ec2 is the Kind-only EC2 endpoint the
// cluster-controller calls through AWS_ENDPOINT_URL_EC2
// (infra/space/kind/charts/space-kind-fake-ec2). It answers two calls from
// the cluster's Node objects, which stand in for the instances behind them:
//
//   - DescribeInstances filtered on one private-dns-name, which the
//     reconcile-queue GC's Ec2Detector asks for the instance behind a
//     non-ready node's Kubernetes name (on EKS, the instance's private DNS
//     name);
//   - DescribeInstances filtered on one instance-id, which the v2 node drain
//     asks for the instance a node's row records. A v2 drain scenario gives
//     its Kind worker a fake AWS providerID (aws:///<zone>/i-<17 hex>):
//     space_smoke's set_kind_worker_provider_id re-registers the worker's
//     Node under it, so its instance has an ID to ask about;
//   - Auto Scaling's TerminateInstanceInAutoScalingGroup, which the
//     node-drain `aws` terminator calls through AWS_ENDPOINT_URL_AUTO_SCALING
//     (the same listener) for the instance behind a Node's providerID.
//
// A Node's instance is the ID its AWS providerID names, or, without one, an
// ID derived from the Node's UID. Its state:
//
//   - shutting-down once the Auto Scaling terminate or the test control
//     named it, for the delay the test control set before it (none by
//     default), then terminated;
//   - running when its Node's Ready condition is True;
//   - stopped for any other Node (NotReady, Unknown);
//   - no instance otherwise, as EC2 answers once a terminated instance has
//     aged out of DescribeInstances;
//   - a Kubernetes API error is an EC2 error (Unavailable, HTTP 503).
//
// TerminateInstanceInAutoScalingGroup only marks the instance terminated: the harness stops the worker's container itself,
// once it sees the termination the controller recorded. The marks live in
// memory, so a restart of the fixture forgets them.
//
// The test control listens on loopback only, in plain HTTP, so only a
// process in the pod reaches it: `space-kind-fake-ec2 terminate <id>...`,
// `space-kind-fake-ec2 delay <seconds>`, `space-kind-fake-ec2 release <id>`
// and `space-kind-fake-ec2 state <id>`, run through `kubectl exec`, post to
// it. The delay applies to the terminations marked after it is set, so a
// scenario sets it for itself and puts 0 back. Release ends the delay of one
// instance already marked, which then reads terminated; state prints what
// DescribeInstances answers for one instance ID.
//
// It checks that a request is signed for EC2, or for Auto Scaling, in the
// cluster's region, but not the signature itself, and refuses every other action, parameter and
// filter, so a call the controller starts making shows up as an error rather
// than a made-up answer. Each request is logged, which is the record that the
// controller's calls stayed in the cluster.
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
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	ec2NS          = "http://ec2.amazonaws.com/doc/2016-11-15/"
	ec2Version     = "2016-11-15"
	asgNS          = "http://autoscaling.amazonaws.com/doc/2011-01-01/"
	asgVersion     = "2011-01-01"
	serviceAccount = "/var/run/secrets/kubernetes.io/serviceaccount"
)

// nodeName is a DNS-1123 subdomain, the form of every Node name.
var nodeName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$`)

// instanceIDForm is an EC2 instance ID: i- and 8 or 17 lowercase hex digits.
var instanceIDForm = regexp.MustCompile(`^i-([0-9a-f]{8}|[0-9a-f]{17})$`)

// defaultControl is the test control's loopback address.
const defaultControl = "127.0.0.1:8081"

func main() {
	var err error
	switch {
	case len(os.Args) > 1 && os.Args[1] == "terminate":
		err = terminateCommand(os.Args[2:])
	case len(os.Args) > 1 && os.Args[1] == "delay":
		err = delayCommand(os.Args[2:])
	case len(os.Args) > 1 && (os.Args[1] == "release" || os.Args[1] == "state"):
		err = instanceCommand(os.Args[1], os.Args[2:])
	default:
		err = run(os.Args[1:])
	}
	if err != nil {
		log.Fatal(err)
	}
}

// terminateCommand asks the running fixture's test control to mark each
// instance terminated.
func terminateCommand(args []string) error {
	fs := flag.NewFlagSet("space-kind-fake-ec2 terminate", flag.ExitOnError)
	control := fs.String("control", defaultControl, "the running fixture's test control address")
	_ = fs.Parse(args)
	if fs.NArg() == 0 {
		return errors.New("usage: space-kind-fake-ec2 terminate [-control addr] <instance-id>...")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	for _, id := range fs.Args() {
		resp, err := client.PostForm("http://"+*control+"/terminate", url.Values{"instance-id": {id}})
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("terminate %s: HTTP %d: %s", id, resp.StatusCode, strings.TrimSpace(string(body)))
		}
		fmt.Printf("%s terminated\n", id)
	}
	return nil
}

// maxDelay bounds the delay before a terminated instance reads terminated.
const maxDelay = time.Hour

// delayCommand asks the running fixture's test control to keep each
// instance it marks terminated from now on shutting-down for that many
// seconds first.
func delayCommand(args []string) error {
	fs := flag.NewFlagSet("space-kind-fake-ec2 delay", flag.ExitOnError)
	control := fs.String("control", defaultControl, "the running fixture's test control address")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: space-kind-fake-ec2 delay [-control addr] <seconds>")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.PostForm("http://"+*control+"/delay", url.Values{"seconds": {fs.Arg(0)}})
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("delay %s: HTTP %d: %s", fs.Arg(0), resp.StatusCode, strings.TrimSpace(string(body)))
	}
	fmt.Printf("terminations now wait %ss before terminated\n", fs.Arg(0))
	return nil
}

// instanceCommand posts one instance ID to the running fixture's test
// control: `release` ends its delay, so it reads terminated now; `state`
// prints what DescribeInstances answers for it.
func instanceCommand(name string, args []string) error {
	fs := flag.NewFlagSet("space-kind-fake-ec2 "+name, flag.ExitOnError)
	control := fs.String("control", defaultControl, "the running fixture's test control address")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: space-kind-fake-ec2 %s [-control addr] <instance-id>", name)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.PostForm("http://"+*control+"/"+name, url.Values{"instance-id": {fs.Arg(0)}})
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s %s: HTTP %d: %s", name, fs.Arg(0), resp.StatusCode, strings.TrimSpace(string(body)))
	}
	fmt.Print(string(body))
	return nil
}

func run(args []string) error {
	fs := flag.NewFlagSet("space-kind-fake-ec2", flag.ExitOnError)
	listen := fs.String("listen", ":8443", "listen address (HTTPS), the cluster-controller's AWS_ENDPOINT_URL_EC2")
	cert := fs.String("tls-cert", "", "listener certificate chain, leaf first")
	key := fs.String("tls-key", "", "listener key")
	region := fs.String("region", "", "the region requests must be signed for (the cluster-controller's AWS_REGION)")
	control := fs.String("control-listen", defaultControl, "the test control's address; loopback only")
	_ = fs.Parse(args)
	if host, _, err := net.SplitHostPort(*control); err != nil || !net.ParseIP(host).IsLoopback() {
		return fmt.Errorf("-control-listen %q is not a loopback address", *control)
	}
	for _, f := range []struct{ name, value string }{
		{"-tls-cert", *cert}, {"-tls-key", *key}, {"-region", *region},
	} {
		if f.value == "" {
			return fmt.Errorf("%s is required", f.name)
		}
	}
	nodes, list, err := inClusterNodes()
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	fake := &fakeEC2{region: *region, nodes: nodes, list: list, log: logger, state: newInstances()}
	controlMux := http.NewServeMux()
	controlMux.HandleFunc("POST /terminate", fake.controlTerminate)
	controlMux.HandleFunc("POST /delay", fake.controlDelay)
	controlMux.HandleFunc("POST /release", fake.controlRelease)
	controlMux.HandleFunc("POST /state", fake.controlState)
	controlSrv := &http.Server{Addr: *control, Handler: controlMux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := controlSrv.ListenAndServe(); err != nil {
			log.Fatalf("test control on %s: %v", *control, err)
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.Handle("POST /", fake)
	srv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    16 << 10,
	}
	log.Printf("ec2 on %s for region %s, test control on %s", *listen, *region, *control)
	return srv.ListenAndServeTLS(*cert, *key)
}

// node is the part of a Kubernetes Node the answer depends on.
type node struct {
	Metadata struct {
		Name string `json:"name"`
		UID  string `json:"uid"`
	} `json:"metadata"`
	Spec struct {
		ProviderID string `json:"providerID"`
	} `json:"spec"`
	Status struct {
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
	} `json:"status"`
}

// nodeGetter reads one Node: nil and no error when it does not exist.
type nodeGetter func(name string) (*node, error)

// nodeLister reads every Node.
type nodeLister func() ([]node, error)

// inClusterNodes reads Nodes from the API server with the pod's ServiceAccount.
func inClusterNodes() (nodeGetter, nodeLister, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, nil, errors.New("not running in a cluster: KUBERNETES_SERVICE_HOST/PORT are unset")
	}
	ca, err := os.ReadFile(serviceAccount + "/ca.crt")
	if err != nil {
		return nil, nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, nil, errors.New("the ServiceAccount CA holds no PEM certificate")
	}
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}},
	}
	base := "https://" + net.JoinHostPort(host, port) + "/api/v1/nodes"
	get := func(path string, into any) (bool, error) {
		// Re-read per request: the kubelet rotates the projected token.
		token, err := os.ReadFile(serviceAccount + "/token")
		if err != nil {
			return false, err
		}
		req, err := http.NewRequest(http.MethodGet, base+path, nil)
		if err != nil {
			return false, err
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
		resp, err := client.Do(req)
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusOK:
			return true, json.NewDecoder(resp.Body).Decode(into)
		case http.StatusNotFound:
			return false, nil
		default:
			return false, fmt.Errorf("GET nodes%s: HTTP %d", path, resp.StatusCode)
		}
	}
	getter := func(name string) (*node, error) {
		var n node
		found, err := get("/"+url.PathEscape(name), &n)
		if err != nil || !found {
			return nil, err
		}
		return &n, nil
	}
	lister := func() ([]node, error) {
		var list struct {
			Items []node `json:"items"`
		}
		if _, err := get("", &list); err != nil {
			return nil, err
		}
		return list.Items, nil
	}
	return getter, lister, nil
}

// instances is what the fixture remembers: the instances it was told are
// terminated, each with when it reads terminated, and the delay the next
// ones wait.
type instances struct {
	mu         sync.Mutex
	terminated map[string]time.Time
	delay      time.Duration
}

func newInstances() *instances {
	return &instances{terminated: map[string]time.Time{}}
}

func (s *instances) setDelay(delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delay = delay
}

// release ends the delay of id, marked terminated, so it reads terminated
// from now on; false when id was never marked.
func (s *instances) release(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok := s.terminated[id]
	if !ok {
		return false
	}
	if now := time.Now(); now.Before(at) {
		s.terminated[id] = now
	}
	return true
}

// marked is a terminated instance's state: shutting-down until its delay
// has passed, then terminated; false when the instance was never marked.
func (s *instances) marked(id string) (instanceState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok := s.terminated[id]
	if !ok {
		return instanceState{}, false
	}
	if time.Now().Before(at) {
		return instanceState{Code: 32, Name: "shutting-down"}, true
	}
	return instanceState{Code: 48, Name: "terminated"}, true
}

// terminate marks id terminated after the current delay, returning its
// state before. A mark already made stays as it was.
func (s *instances) terminate(id string, before string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.terminated[id]; ok {
		return "terminated"
	}
	s.terminated[id] = time.Now().Add(s.delay)
	return before
}

type fakeEC2 struct {
	region string
	nodes  nodeGetter
	list   nodeLister
	log    *slog.Logger
	state  *instances
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
	form := r.PostForm
	entry = append(entry, "action", form.Get("Action"))
	switch scope := credentialScope(r.Header.Get("Authorization")); scope {
	case f.region + "/ec2/aws4_request":
	case f.region + "/autoscaling/aws4_request":
		f.serveAutoScaling(w, form, reqID, entry, fail)
		return
	default:
		fail(http.StatusUnauthorized, "AuthFailure", fmt.Sprintf("request is not signed for ec2 or autoscaling in %s (scope %q)", f.region, scope))
		return
	}
	if form.Get("Version") != ec2Version {
		fail(http.StatusBadRequest, "InvalidAction", "this fake answers only version "+ec2Version)
		return
	}
	switch form.Get("Action") {
	case "DescribeInstances":
		switch form.Get("Filter.1.Name") {
		case "private-dns-name":
			f.describeByPrivateDNS(w, form, reqID, entry, fail)
		case "instance-id":
			f.describeByInstanceID(w, form, reqID, entry, fail)
		default:
			fail(http.StatusBadRequest, "InvalidParameterValue", "this fake answers only a private-dns-name or instance-id filter")
		}
	default:
		fail(http.StatusBadRequest, "InvalidAction", "this fake answers only DescribeInstances for EC2")
	}
}

type failFunc func(status int, code, message string)

type terminateInAutoScalingGroupResponse struct {
	XMLName  xml.Name `xml:"TerminateInstanceInAutoScalingGroupResponse"`
	Xmlns    string   `xml:"xmlns,attr"`
	Activity struct {
		ActivityID  string `xml:"ActivityId"`
		Description string `xml:"Description"`
		StatusCode  string `xml:"StatusCode"`
	} `xml:"TerminateInstanceInAutoScalingGroupResult>Activity"`
	RequestID string `xml:"ResponseMetadata>RequestId"`
}

// serveAutoScaling answers the one Auto Scaling call the fixture takes:
// TerminateInstanceInAutoScalingGroup for one instance, marked terminated as
// the test control marks it.
func (f *fakeEC2) serveAutoScaling(w http.ResponseWriter, form url.Values, reqID string, entry []any, fail failFunc) {
	if form.Get("Version") != asgVersion {
		fail(http.StatusBadRequest, "InvalidAction", "this fake answers Auto Scaling only at version "+asgVersion)
		return
	}
	if form.Get("Action") != "TerminateInstanceInAutoScalingGroup" {
		fail(http.StatusBadRequest, "InvalidAction", "this fake answers only TerminateInstanceInAutoScalingGroup for Auto Scaling")
		return
	}
	for param := range form {
		switch param {
		case "Action", "Version", "InstanceId", "ShouldDecrementDesiredCapacity":
		default:
			fail(http.StatusBadRequest, "ValidationError", "unsupported parameter "+param)
			return
		}
	}
	id := form.Get("InstanceId")
	if !instanceIDForm.MatchString(id) {
		fail(http.StatusBadRequest, "ValidationError", "not an instance ID: "+id)
		return
	}
	switch decrement := form.Get("ShouldDecrementDesiredCapacity"); decrement {
	case "true", "false":
	default:
		fail(http.StatusBadRequest, "ValidationError", "ShouldDecrementDesiredCapacity must be true or false, not "+strconv.Quote(decrement))
		return
	}
	before := f.terminate(id)
	resp := terminateInAutoScalingGroupResponse{Xmlns: asgNS, RequestID: reqID}
	resp.Activity.ActivityID = reqID
	resp.Activity.Description = "Terminating EC2 instance: " + id
	resp.Activity.StatusCode = "InProgress"
	f.log.Info("terminated in auto scaling group", append(entry, "status", http.StatusOK, "instance_id", id,
		"previous_state", before.Name, "decrement_capacity", form.Get("ShouldDecrementDesiredCapacity"))...)
	writeXML(w, http.StatusOK, resp)
}

// describeByPrivateDNS answers a filter on one private-dns-name from the Node
// of that name.
func (f *fakeEC2) describeByPrivateDNS(w http.ResponseWriter, form url.Values, reqID string, entry []any, fail failFunc) {
	name := form.Get("Filter.1.Value.1")
	for param := range form {
		switch param {
		case "Action", "Version", "Filter.1.Name", "Filter.1.Value.1", "MaxResults", "NextToken":
		default:
			fail(http.StatusBadRequest, "InvalidParameterValue", "unsupported parameter "+param)
			return
		}
	}
	if !nodeName.MatchString(name) {
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
		inst := f.instanceOf(n)
		inst.PrivateDNSName = name
		resp.Reservations = []reservation{{ReservationID: "r-" + inst.InstanceID[2:], Instances: []instance{inst}}}
		f.log.Info("answered", append(entry, "status", http.StatusOK, "answer", inst.InstanceState.Name,
			"instance_id", inst.InstanceID)...)
	}
	writeXML(w, http.StatusOK, resp)
}

// describeByInstanceID answers a filter on one instance ID from the Nodes
// and the instances marked terminated.
func (f *fakeEC2) describeByInstanceID(w http.ResponseWriter, form url.Values, reqID string, entry []any, fail failFunc) {
	id := form.Get("Filter.1.Value.1")
	for param := range form {
		switch param {
		case "Action", "Version", "Filter.1.Name", "Filter.1.Value.1", "MaxResults":
		default:
			fail(http.StatusBadRequest, "InvalidParameterValue", "this fake answers only a filter on one instance-id, unsupported parameter "+param)
			return
		}
	}
	if !instanceIDForm.MatchString(id) {
		fail(http.StatusBadRequest, "InvalidParameterValue", "not an instance ID: "+id)
		return
	}
	if raw := form.Get("MaxResults"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 5 || n > 1000 {
			fail(http.StatusBadRequest, "InvalidParameterValue", "MaxResults must be 5 to 1000")
			return
		}
	}
	matched, err := f.instancesByID(id)
	if err != nil {
		fail(http.StatusServiceUnavailable, "Unavailable", "the Kubernetes API did not answer: "+err.Error())
		return
	}
	resp := describeInstancesResponse{Xmlns: ec2NS, RequestID: reqID}
	for _, inst := range matched {
		resp.Reservations = append(resp.Reservations, reservation{ReservationID: "r-" + inst.InstanceID[2:], Instances: []instance{inst}})
	}
	f.log.Info("answered", append(entry, "status", http.StatusOK, "instance_id", id,
		"answer", answerOf(matched))...)
	writeXML(w, http.StatusOK, resp)
}

// instancesByID is what DescribeInstances lists for one instance ID: the
// instances behind the Nodes, or, with none, the instance marked
// terminated.
func (f *fakeEC2) instancesByID(id string) ([]instance, error) {
	nodes, err := f.list()
	if err != nil {
		return nil, err
	}
	var matched []instance
	for i := range nodes {
		if inst := f.instanceOf(&nodes[i]); inst.InstanceID == id {
			matched = append(matched, inst)
		}
	}
	if st, ok := f.state.marked(id); ok && len(matched) == 0 {
		matched = append(matched, instance{InstanceID: id, InstanceState: st})
	}
	return matched, nil
}

// answerOf is the states listed, comma-separated, or "no instance".
func answerOf(matched []instance) string {
	if len(matched) == 0 {
		return "no instance"
	}
	answer := make([]string, 0, len(matched))
	for _, inst := range matched {
		answer = append(answer, inst.InstanceState.Name)
	}
	return strings.Join(answer, ",")
}

// terminate marks id terminated and returns its state before.
func (f *fakeEC2) terminate(id string) instanceState {
	before := "stopped"
	if nodes, err := f.list(); err == nil {
		for i := range nodes {
			if inst := f.instanceOf(&nodes[i]); inst.InstanceID == id {
				before = inst.InstanceState.Name
			}
		}
	}
	switch f.state.terminate(id, before) {
	case "running":
		return instanceState{Code: 16, Name: "running"}
	case "terminated":
		return instanceState{Code: 48, Name: "terminated"}
	default:
		return instanceState{Code: 80, Name: "stopped"}
	}
}

// controlTerminate is the test control's terminate: the same mark, for a
// harness that reaches the fixture through `kubectl exec`.
func (f *fakeEC2) controlTerminate(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4<<10)
	id := r.FormValue("instance-id")
	if !instanceIDForm.MatchString(id) {
		http.Error(w, "not an instance ID: "+id, http.StatusBadRequest)
		return
	}
	before := f.terminate(id)
	f.log.Info("terminated by test control", "instance_id", id, "previous_state", before.Name)
	_, _ = fmt.Fprintf(w, "%s terminated\n", id)
}

// controlRelease is the test control's release: the instance marked
// terminated reads terminated from now on.
func (f *fakeEC2) controlRelease(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4<<10)
	id := r.FormValue("instance-id")
	if !instanceIDForm.MatchString(id) {
		http.Error(w, "not an instance ID: "+id, http.StatusBadRequest)
		return
	}
	if !f.state.release(id) {
		http.Error(w, id+" was never marked terminated", http.StatusConflict)
		return
	}
	f.log.Info("released by test control", "instance_id", id)
	_, _ = fmt.Fprintf(w, "%s released\n", id)
}

// controlState is the test control's state: what DescribeInstances lists
// for one instance ID.
func (f *fakeEC2) controlState(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4<<10)
	id := r.FormValue("instance-id")
	if !instanceIDForm.MatchString(id) {
		http.Error(w, "not an instance ID: "+id, http.StatusBadRequest)
		return
	}
	matched, err := f.instancesByID(id)
	if err != nil {
		http.Error(w, "the Kubernetes API did not answer: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	_, _ = fmt.Fprintf(w, "%s\n", answerOf(matched))
}

// controlDelay is the test control's delay: how long each instance marked
// terminated from now on reads shutting-down first.
func (f *fakeEC2) controlDelay(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4<<10)
	seconds, err := strconv.Atoi(r.FormValue("seconds"))
	if err != nil || seconds < 0 || time.Duration(seconds)*time.Second > maxDelay {
		http.Error(w, "seconds must be 0 to 3600", http.StatusBadRequest)
		return
	}
	f.state.setDelay(time.Duration(seconds) * time.Second)
	f.log.Info("termination delay set by test control", "seconds", seconds)
	_, _ = fmt.Fprintf(w, "delay %ds\n", seconds)
}

// instanceOf is the instance behind n: its AWS providerID's instance, or one
// derived from its UID, terminated once marked.
func (f *fakeEC2) instanceOf(n *node) instance {
	id := providerInstanceID(n.Spec.ProviderID)
	if id == "" {
		id = instanceID(n.Metadata.UID)
	}
	st := state(n)
	if marked, ok := f.state.marked(id); ok {
		st = marked
	}
	return instance{InstanceID: id, InstanceState: st, PrivateDNSName: n.Metadata.Name}
}

// providerInstanceID is the instance an AWS providerID (aws:///<zone>/<id>)
// names, or "" for any other.
func providerInstanceID(providerID string) string {
	rest, ok := strings.CutPrefix(providerID, "aws:///")
	if !ok {
		return ""
	}
	id := rest[strings.LastIndex(rest, "/")+1:]
	if !instanceIDForm.MatchString(id) {
		return ""
	}
	return id
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
