package pintle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Noot-tooN/Pintle/gen/go/pintle/v1"
	"github.com/Noot-tooN/Pintle/gen/go/pintle/v1/pintlev1connect"
)

type Resource struct{ Namespace, Key string }
type Lease struct {
	// Zero for mutexes; semaphore fencing is scoped to Resource + PermitSlot.
	PermitSlot, Capacity     uint32
	Resource                 Resource
	AcquisitionID, HolderRef string
	// LeaseID is a secret ownership capability. Do not log or expose it.
	LeaseID                string
	FencingToken, Revision string
	// Server observation only. Client wall time is not an ownership oracle.
	ExpiresAtUnixMS uint64
}

func (l Lease) String() string {
	return fmt.Sprintf("Lease{%s/%s fence=%s capability=REDACTED}", l.Resource.Namespace, l.Resource.Key, l.FencingToken)
}

func (l Lease) GoString() string { return l.String() }

type State int

const (
	Unspecified State = iota
	Active
	Expired
	Released
	Superseded
)

type GrantResult struct {
	// Receipt is the immutable historical result of this logical operation.
	Receipt *Lease
	// Current is the latest lease, only when active at the server observation.
	Current             *Lease
	State               State
	Replayed, Contended bool
}
type ReleaseResult struct {
	Revision string
	Replayed bool
}
type Status struct {
	Waiting           WaitState
	Acquired, Renewed *GrantResult
	Released          *ReleaseResult
}
type AcquireRequest struct {
	// Zero for mutexes. Set by a semaphore-bound Session.
	Capacity             uint32
	Attempt              *Attempt
	Resource             Resource
	RequestID, HolderRef string
	TTL                  time.Duration
}
type RenewRequest struct {
	Attempt   *Attempt
	Lease     Lease
	RequestID string
	TTL       time.Duration
}
type ReleaseRequest struct {
	Attempt   *Attempt
	Lease     Lease
	RequestID string
}

// NewRequestID uses the standard library CSPRNG. Persist the resulting request
// before sending if recovery across application restarts is required.
func NewRequestID() (string, error) {
	var b [32]byte
	_, err := rand.Read(b[:])
	return hex.EncodeToString(b[:]), err
}

// NewAcquire prepares a low-level request without a recovery session. For the
// current service use Session.NewAcquire instead; a session attempt is required.
func NewAcquire(resource Resource, holder string, ttl time.Duration) (AcquireRequest, error) {
	if !validResource(resource) || !identifier(holder, 64) {
		return AcquireRequest{}, fmt.Errorf("invalid resource or holder")
	}
	id, err := NewRequestID()
	return AcquireRequest{Resource: resource, RequestID: id, HolderRef: holder, TTL: ttl}, err
}

// NewRenew prepares a low-level request without a recovery session. Use
// Session.NewRenew for the current service.
func NewRenew(lease Lease, ttl time.Duration) (RenewRequest, error) {
	if err := validateRenewalFence(lease); err != nil {
		return RenewRequest{}, err
	}
	if err := validateOwnership(lease); err != nil {
		return RenewRequest{}, err
	}
	id, err := NewRequestID()
	return RenewRequest{Lease: lease, RequestID: id, TTL: ttl}, err
}

// NewRelease prepares a low-level request without a recovery session. Use
// Session.NewRelease for the current service.
func NewRelease(lease Lease) (ReleaseRequest, error) {
	if err := validateOwnership(lease); err != nil {
		return ReleaseRequest{}, err
	}
	id, err := NewRequestID()
	return ReleaseRequest{Lease: lease, RequestID: id}, err
}

func validateRenewalFence(l Lease) error {
	if _, err := decimal(l.FencingToken, true); err != nil {
		return fmt.Errorf("renewal requires a canonical positive fencing token")
	}
	return nil
}

func (c *Client) checkRenewal(l Lease) error {
	if err := validateRenewalFence(l); err != nil {
		return err
	}
	return c.checkLease(l)
}

func validateOwnership(l Lease) error {
	capability, err := hex.DecodeString(l.LeaseID)
	if !validPermit(l.PermitSlot, l.Capacity) || !validResource(l.Resource) || err != nil || len(capability) != 32 || hex.EncodeToString(capability) != l.LeaseID {
		return fmt.Errorf("valid resource and canonical lease capability required")
	}
	return nil
}

type Client struct {
	flightRPC     pintlev1connect.SingleflightServiceClient
	semaphoreRPC  pintlev1connect.SemaphoreServiceClient
	capacity      uint32
	rpc           pintlev1connect.LockServiceClient
	credential    string
	leaseSessions *leaseSessionCache
}

// Value receivers also redact Client values copied into debug containers.
func (c Client) String() string   { return "Client{credential=REDACTED}" }
func (c Client) GoString() string { return c.String() }

// New creates a reusable client for a Pintle endpoint with a bearer credential.
// HTTPS is required outside loopback. The default HTTP client refuses redirects.
func New(endpoint, credential string) (*Client, error) {
	return NewWithHTTPClient(endpoint, credential, nil)
}

// NewWithHTTPClient uses the supplied HTTP client for all RPCs. It must honor
// context cancellation and refuse redirects to protect the bearer credential.
// A nil HTTP client selects the same default as New.
func NewWithHTTPClient(endpoint, credential string, httpClient connect.HTTPClient) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || credential == "" || len(credential) > 256 || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("endpoint and credential required")
	}
	if u.Scheme == "http" && u.Hostname() != "localhost" && !net.ParseIP(u.Hostname()).IsLoopback() {
		return nil, fmt.Errorf("HTTPS required outside loopback")
	}
	for _, b := range []byte(credential) {
		if b < 33 || b > 126 {
			return nil, fmt.Errorf("invalid credential")
		}
	}
	if httpClient == nil {
		httpClient = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	limit := connect.WithReadMaxBytes(16384)
	return &Client{
		flightRPC:    pintlev1connect.NewSingleflightServiceClient(httpClient, endpoint, limit),
		semaphoreRPC: pintlev1connect.NewSemaphoreServiceClient(httpClient, endpoint, limit),
		rpc:          pintlev1connect.NewLockServiceClient(httpClient, endpoint, limit),
		credential:   credential, leaseSessions: &leaseSessionCache{},
	}, nil
}
func resource(r Resource) *v1.ResourceId { return &v1.ResourceId{Namespace: r.Namespace, Key: r.Key} }
func ttl(d time.Duration) (uint32, error) {
	if d < 100*time.Millisecond || d > 60*time.Second || d%time.Millisecond != 0 {
		return 0, fmt.Errorf("TTL must be whole milliseconds in [100,60000]")
	}
	return uint32(d / time.Millisecond), nil
}
func authorize[T any](c *Client, msg *T) *connect.Request[T] {
	r := connect.NewRequest(msg)
	r.Header().Set("Authorization", "Bearer "+c.credential)
	return r
}
func decodeLease(l *v1.GrantReceipt) (*Lease, error) {
	if l == nil || l.Resource == nil {
		return nil, fmt.Errorf("missing lease receipt")
	}
	secret, err := hex.DecodeString(l.LeaseId)
	if err != nil || len(secret) != 32 || hex.EncodeToString(secret) != l.LeaseId {
		return nil, fmt.Errorf("invalid lease capability")
	}
	for _, value := range []string{l.FencingToken, l.Revision, l.ExpiresAtUnixMs} {
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil || n == 0 || strconv.FormatUint(n, 10) != value {
			return nil, fmt.Errorf("invalid lease counter/time")
		}
	}
	expires, _ := strconv.ParseUint(l.ExpiresAtUnixMs, 10, 64)
	if !validPermit(l.PermitSlot, l.SemaphoreCapacity) || expires > maxAuthorityTime || !validResource(Resource{l.Resource.Namespace, l.Resource.Key}) || !identifier(l.RequestId, 128) || !identifier(l.HolderRef, 64) {
		return nil, fmt.Errorf("invalid lease identity/time")
	}
	return &Lease{PermitSlot: l.PermitSlot, Capacity: l.SemaphoreCapacity, Resource: Resource{l.Resource.Namespace, l.Resource.Key}, AcquisitionID: l.RequestId, HolderRef: l.HolderRef, LeaseID: l.LeaseId, FencingToken: l.FencingToken, Revision: l.Revision, ExpiresAtUnixMS: expires}, nil
}
func grant(l *v1.GrantReceipt, replayed bool, o *v1.LeaseObservation) (*GrantResult, error) {
	receipt, err := decodeLease(l)
	if err != nil {
		return nil, err
	}
	if o == nil || o.State < v1.LeaseState_LEASE_STATE_ACTIVE || o.State > v1.LeaseState_LEASE_STATE_SUPERSEDED {
		return nil, fmt.Errorf("missing lease state")
	}
	var current *Lease
	if o.State == v1.LeaseState_LEASE_STATE_ACTIVE {
		current, err = decodeLease(o.Current)
		if err != nil {
			return nil, err
		}
		currentRevision, _ := decimal(current.Revision, true)
		receiptRevision, _ := decimal(receipt.Revision, true)
		if current.PermitSlot != receipt.PermitSlot || current.Capacity != receipt.Capacity || current.LeaseID != receipt.LeaseID || current.Resource != receipt.Resource || current.FencingToken != receipt.FencingToken || current.AcquisitionID != receipt.AcquisitionID || current.HolderRef != receipt.HolderRef || currentRevision < receiptRevision || current.ExpiresAtUnixMS < receipt.ExpiresAtUnixMS {
			return nil, fmt.Errorf("mismatched lease observation")
		}
	} else if o.Current != nil {
		return nil, fmt.Errorf("inactive lease includes current owner")
	}
	return &GrantResult{Receipt: receipt, Current: current, State: State(o.State), Replayed: replayed}, nil
}
func acquired(r *v1.TryAcquireResponse) (*GrantResult, error) {
	if r == nil {
		return nil, fmt.Errorf("missing acquire response")
	}
	if r.GetContended() != nil {
		return &GrantResult{Contended: true}, nil
	}
	return grant(r.GetAcquired(), r.Replayed, r.Observation)
}
func (c *Client) TryAcquire(ctx context.Context, r AcquireRequest) (*GrantResult, error) {
	if r.Capacity != c.capacity {
		return nil, fmt.Errorf("acquisition capacity does not match client")
	}
	ms, err := ttl(r.TTL)
	if err != nil {
		return nil, err
	}
	res, err := c.rpc.TryAcquire(ctx, authorize(c, &v1.TryAcquireRequest{SemaphoreCapacity: r.Capacity, Resource: resource(r.Resource), RequestId: r.RequestID, HolderRef: r.HolderRef, LeaseTtlMs: ms, Attempt: wireAttempt(r.Attempt)}))
	if err != nil {
		return nil, err
	}
	g, err := acquired(res.Msg)
	if err == nil && !g.Contended && (g.Receipt.Capacity != c.capacity || g.Receipt.Resource != r.Resource || g.Receipt.AcquisitionID != r.RequestID || g.Receipt.HolderRef != r.HolderRef) {
		return nil, fmt.Errorf("response does not match acquisition")
	}
	return g, err
}
func (c *Client) Renew(ctx context.Context, r RenewRequest) (*GrantResult, error) {
	if err := c.checkRenewal(r.Lease); err != nil {
		return nil, err
	}
	ms, err := ttl(r.TTL)
	if err != nil {
		return nil, err
	}
	res, err := c.rpc.Renew(ctx, authorize(c, &v1.RenewRequest{Resource: resource(r.Lease.Resource), RequestId: r.RequestID, LeaseId: r.Lease.LeaseID, LeaseTtlMs: ms, Attempt: wireAttempt(r.Attempt)}))
	if err != nil {
		return nil, err
	}
	g, err := grant(res.Msg.Lease, res.Msg.Replayed, res.Msg.Observation)
	if err == nil && (g.Receipt.PermitSlot != r.Lease.PermitSlot || g.Receipt.Capacity != r.Lease.Capacity || g.Receipt.Resource != r.Lease.Resource || g.Receipt.LeaseID != r.Lease.LeaseID || g.Receipt.FencingToken != r.Lease.FencingToken) {
		return nil, fmt.Errorf("response does not match renewal")
	}
	return g, err
}
func (c *Client) Release(ctx context.Context, r ReleaseRequest) (*ReleaseResult, error) {
	if err := c.checkLease(r.Lease); err != nil {
		return nil, err
	}
	res, err := c.rpc.Release(ctx, authorize(c, &v1.ReleaseRequest{Resource: resource(r.Lease.Resource), RequestId: r.RequestID, LeaseId: r.Lease.LeaseID, Attempt: wireAttempt(r.Attempt)}))
	if err != nil {
		return nil, err
	}
	if _, err := decimal(res.Msg.Revision, true); err != nil {
		return nil, err
	}
	return &ReleaseResult{Revision: res.Msg.Revision, Replayed: res.Msg.Replayed}, nil
}

// GetRequestStatus observes committed state. NotFound does not cancel an in-flight
// request. Continue reconciliation with its SAME ID; never create a replacement ID.
func (c *Client) GetRequestStatus(ctx context.Context, r Resource, id string) (*Status, error) {
	return c.GetAttemptStatus(ctx, r, id, nil)
}

func (c *Client) GetAttemptStatus(ctx context.Context, r Resource, id string, attempt *Attempt) (*Status, error) {
	res, err := c.rpc.GetRequestStatus(ctx, authorize(c, &v1.GetRequestStatusRequest{Resource: resource(r), RequestId: id, Attempt: wireAttempt(attempt)}))
	if err != nil {
		return nil, err
	}
	s := &Status{}
	switch outcome := res.Msg.Outcome.(type) {
	case *v1.GetRequestStatusResponse_Waiting:
		if outcome.Waiting < v1.WaitState_WAIT_STATE_PENDING || outcome.Waiting > v1.WaitState_WAIT_STATE_EXPIRED {
			return nil, fmt.Errorf("invalid wait state")
		}
		s.Waiting = WaitState(outcome.Waiting)
	case *v1.GetRequestStatusResponse_Acquired:
		s.Acquired, err = acquired(outcome.Acquired)
	case *v1.GetRequestStatusResponse_Renewed:
		s.Renewed, err = grant(outcome.Renewed.Lease, outcome.Renewed.Replayed, outcome.Renewed.Observation)
	case *v1.GetRequestStatusResponse_Released:
		if _, err = decimal(outcome.Released.Revision, true); err != nil {
			return nil, err
		}
		s.Released = &ReleaseResult{Revision: outcome.Released.Revision, Replayed: outcome.Released.Replayed}
	default:
		err = fmt.Errorf("missing request status")
	}
	if err == nil && s.Acquired != nil && !s.Acquired.Contended && (s.Acquired.Receipt.Capacity != c.capacity || s.Acquired.Receipt.Resource != r || s.Acquired.Receipt.AcquisitionID != id) {
		return nil, fmt.Errorf("status does not match acquisition")
	}
	if err == nil && s.Renewed != nil && (s.Renewed.Receipt.Capacity != c.capacity || s.Renewed.Receipt.Resource != r) {
		return nil, fmt.Errorf("status does not match resource")
	}
	return s, err
}
