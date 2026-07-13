// Package grpcidem adds Idempotency-Key semantics to unary gRPC servers,
// backed by the idemlease state machine.
//
// It shares the core (Store + Begin/Finish) with the HTTP integration
// but depends only on idemlease, grpc, and protobuf — never net/http
// (REQUIREMENTS §9.4). The key comes from request metadata, the
// fingerprint from the marshaled request message, and the stored
// payload from the marshaled response message; on replay the response
// type is recovered from the global proto registry, so no per-method
// registration is needed.
//
//	srv := grpc.NewServer(
//		grpc.UnaryInterceptor(grpcidem.UnaryServerInterceptor(store,
//			grpcidem.Require(true),
//		)),
//	)
package grpcidem

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/repenguin22/idemlease"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// MetadataKey is the incoming-metadata key carrying the idempotency key
// (gRPC lowercases metadata keys). ReplayedMetadataKey marks a replayed
// response in the response header metadata.
const (
	MetadataKey         = "idempotency-key"
	ReplayedMetadataKey = "idempotency-replayed"
)

const payloadVersion = 0x01

type config struct {
	require   bool
	leaseTTL  time.Duration
	recordTTL time.Duration
	policy    ReplayPolicy
	failOpen  bool
	keyScope  func(context.Context) string
	keyValid  func(string) bool
	logger    *slog.Logger
}

// Option configures the interceptor.
type Option func(*config)

// Require rejects calls without an idempotency-key metadata entry with
// InvalidArgument (default false: they pass through).
func Require(v bool) Option { return func(c *config) { c.require = v } }

// LeaseTTL bounds a single in-flight execution (zero: core default).
func LeaseTTL(d time.Duration) Option { return func(c *config) { c.leaseTTL = d } }

// RecordTTL bounds how long a stored response is replayed (zero: core default).
func RecordTTL(d time.Duration) Option { return func(c *config) { c.recordTTL = d } }

// Policy replaces the default code-driven ReplayPolicy.
func Policy(p ReplayPolicy) Option { return func(c *config) { c.policy = p } }

// FailOpen passes calls through when Begin fails against the store
// (default false: Unavailable).
func FailOpen(v bool) Option { return func(c *config) { c.failOpen = v } }

// KeyScope namespaces keys by caller (§4.5), e.g. an authenticated
// tenant from the call context. Strongly recommended for multi-tenant
// services.
func KeyScope(fn func(context.Context) string) Option {
	return func(c *config) { c.keyScope = fn }
}

// KeyValidator adds validation after grammar checks (rejects with
// InvalidArgument).
func KeyValidator(fn func(string) bool) Option {
	return func(c *config) { c.keyValid = fn }
}

// Logger replaces slog.Default() for warning logs.
func Logger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// UnaryServerInterceptor returns a grpc.UnaryServerInterceptor that
// makes unary methods idempotent using store.
func UnaryServerInterceptor(store idemlease.Store, opts ...Option) grpc.UnaryServerInterceptor {
	cfg := &config{policy: DefaultPolicy, logger: slog.Default()}
	for _, o := range opts {
		o(cfg)
	}
	i := &interceptor{store: store, cfg: cfg}
	return i.intercept
}

type interceptor struct {
	store idemlease.Store
	cfg   *config
}

func (i *interceptor) intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	cfg := i.cfg
	key, err := keyFromContext(ctx)
	if err != nil {
		if errors.Is(err, errKeyMissing) && !cfg.require {
			return handler(ctx, req)
		}
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if cfg.keyValid != nil && !cfg.keyValid(key) {
		return nil, status.Error(codes.InvalidArgument, "grpcidem: idempotency key rejected by validator")
	}

	reqMsg, ok := req.(proto.Message)
	if !ok {
		return nil, status.Errorf(codes.Internal, "grpcidem: request is not a proto.Message (%T)", req)
	}
	reqBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(reqMsg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "grpcidem: marshaling request: %v", err)
	}
	fingerprint := fingerprint(info.FullMethod, reqBytes)

	scope, storeKey := "", key
	if cfg.keyScope != nil {
		if scope = cfg.keyScope(ctx); scope != "" {
			storeKey = scope + "\x00" + key
		}
	}
	o := idemlease.Options{LeaseTTL: cfg.leaseTTL, RecordTTL: cfg.recordTTL}

	out, err := idemlease.Begin(ctx, i.store, storeKey, fingerprint, o)
	if err != nil {
		if cfg.failOpen {
			cfg.logger.Warn("grpcidem: store unavailable; proceeding without idempotency (FailOpen)",
				append(i.keyAttrs(scope, key), slog.Any("error", err))...)
			return handler(ctx, req)
		}
		return nil, status.Error(codes.Unavailable, "grpcidem: idempotency store is unavailable")
	}

	switch out.Action {
	case idemlease.Replay:
		return i.replay(ctx, scope, key, out.Payload)
	case idemlease.RejectInFlight:
		return nil, status.Errorf(codes.Aborted,
			"grpcidem: a request with this idempotency key is in flight; retry after %s", retryAfter(out.RetryAfter))
	case idemlease.RejectFingerprintMismatch:
		return nil, status.Error(codes.FailedPrecondition,
			"grpcidem: idempotency key was already used with a different request")
	}

	// Proceed: run the handler, then Finish. Use a cancellation-free
	// context for Finish so a client disconnect cannot lose the record.
	finishCtx := context.WithoutCancel(ctx)
	resp, herr := i.runHandler(ctx, req, storeKey, out.Token, o, scope, key, finishCtx, handler)
	return resp, herr
}

func (i *interceptor) runHandler(ctx context.Context, req any, storeKey, token string, o idemlease.Options,
	scope, key string, finishCtx context.Context, handler grpc.UnaryHandler) (resp any, herr error) {
	cfg := i.cfg
	defer func() {
		if p := recover(); p != nil {
			if _, ferr := idemlease.Finish(finishCtx, i.store, storeKey, token, idemlease.Discard, nil, o); ferr != nil {
				cfg.logger.Warn("grpcidem: releasing reservation after handler panic failed",
					append(i.keyAttrs(scope, key), slog.Any("error", ferr))...)
			}
			panic(p)
		}
	}()

	resp, herr = handler(ctx, req)

	if payload, ok := i.payloadToPersist(resp, herr, scope, key); ok {
		leaseLost, ferr := idemlease.Finish(finishCtx, i.store, storeKey, token, idemlease.Persist, payload, o)
		i.reportFinish(scope, key, leaseLost, ferr, finishCtx, storeKey, token, o)
		return resp, herr
	}
	if _, ferr := idemlease.Finish(finishCtx, i.store, storeKey, token, idemlease.Discard, nil, o); ferr != nil {
		cfg.logger.Warn("grpcidem: releasing reservation failed; key stays reserved until lease expiry",
			append(i.keyAttrs(scope, key), slog.Any("error", ferr))...)
	}
	return resp, herr
}

// payloadToPersist returns the encoded response payload when the policy
// says persist and the response is a storable proto message; ok is
// false whenever the outcome should be discarded instead.
func (i *interceptor) payloadToPersist(resp any, herr error, scope, key string) (payload []byte, ok bool) {
	// A gRPC error carries no response message to store: the call
	// re-executes on retry (see ReplayPolicy).
	if herr != nil || i.cfg.policy.Decide(herr) != idemlease.Persist {
		return nil, false
	}
	respMsg, isProto := resp.(proto.Message)
	// Guard the typed-nil interface (a nil *Msg wrapped in a non-nil
	// any): its ProtoReflect is invalid and nothing meaningful stores.
	if !isProto || respMsg == nil || !respMsg.ProtoReflect().IsValid() {
		return nil, false
	}
	payload, err := encodePayload(respMsg)
	if err != nil {
		i.cfg.logger.Warn("grpcidem: encoding response failed; discarding",
			append(i.keyAttrs(scope, key), slog.Any("error", err))...)
		return nil, false
	}
	return payload, true
}

func (i *interceptor) reportFinish(scope, key string, leaseLost bool, ferr error,
	finishCtx context.Context, storeKey, token string, o idemlease.Options) {
	cfg := i.cfg
	switch {
	case ferr != nil:
		cfg.logger.Warn("grpcidem: persisting response failed; idempotency temporarily weakened",
			append(i.keyAttrs(scope, key), slog.Any("error", ferr))...)
		if _, rerr := idemlease.Finish(finishCtx, i.store, storeKey, token, idemlease.Discard, nil, o); rerr != nil {
			cfg.logger.Warn("grpcidem: best-effort release failed; key stays reserved until lease expiry",
				append(i.keyAttrs(scope, key), slog.Any("error", rerr))...)
		}
	case leaseLost:
		cfg.logger.Warn("grpcidem: lease lost during execution; response returned but not stored for replay",
			append(i.keyAttrs(scope, key), slog.String("event", "lease_lost"))...)
	}
}

func (i *interceptor) replay(ctx context.Context, scope, key string, payload []byte) (any, error) {
	msg, err := decodePayload(payload)
	if err != nil {
		i.cfg.logger.Error("grpcidem: stored response payload is corrupted",
			append(i.keyAttrs(scope, key), slog.Any("error", err))...)
		return nil, status.Error(codes.Internal, "grpcidem: corrupted stored response")
	}
	_ = grpc.SetHeader(ctx, metadata.Pairs(ReplayedMetadataKey, "true"))
	return msg, nil
}

func (i *interceptor) keyAttrs(scope, key string) []any {
	attrs := []any{slog.String("idempotency_key", key)}
	if scope != "" {
		attrs = append(attrs, slog.String("idempotency_scope", scope))
	}
	return attrs
}

var errKeyMissing = errors.New("grpcidem: idempotency-key metadata is missing")

func keyFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errKeyMissing
	}
	vals := md.Get(MetadataKey)
	switch len(vals) {
	case 0:
		return "", errKeyMissing
	case 1:
		return validateKey(vals[0])
	default:
		return "", fmt.Errorf("grpcidem: %d idempotency-key entries, want exactly 1", len(vals))
	}
}

// validateKey accepts the raw canonical key form: 1-255 bytes with no
// control characters (mirroring httpidem's raw grammar, without the
// HTTP-only RFC 8941 quoting).
func validateKey(k string) (string, error) {
	if len(k) == 0 || len(k) > 255 {
		return "", fmt.Errorf("grpcidem: idempotency key is %d bytes, want 1-255", len(k))
	}
	for i := 0; i < len(k); i++ {
		if c := k[i]; c <= 0x1F || c == 0x7F {
			return "", fmt.Errorf("grpcidem: idempotency key has control byte 0x%02X", c)
		}
	}
	return k, nil
}

// fingerprint is SHA-256(fullMethod + "\n" + requestBytes).
func fingerprint(fullMethod string, reqBytes []byte) []byte {
	h := sha256.New()
	h.Write([]byte(fullMethod))
	h.Write([]byte("\n"))
	h.Write(reqBytes)
	return h.Sum(nil)
}

func retryAfter(d time.Duration) time.Duration {
	if d < time.Second {
		return time.Second
	}
	return d.Round(time.Second)
}

// encodePayload stores the response's proto full name and its
// deterministic marshaling: 0x01 | uvarint(len(name)) | name | body.
func encodePayload(msg proto.Message) ([]byte, error) {
	name := string(msg.ProtoReflect().Descriptor().FullName())
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(name)+len(body))
	buf = append(buf, payloadVersion)
	buf = binary.AppendUvarint(buf, uint64(len(name)))
	buf = append(buf, name...)
	buf = append(buf, body...)
	return buf, nil
}

// decodePayload reconstructs the stored response by looking its type up
// in the global proto registry and unmarshaling into a fresh instance.
func decodePayload(data []byte) (proto.Message, error) {
	if len(data) == 0 || data[0] != payloadVersion {
		return nil, errors.New("grpcidem: unsupported stored payload version")
	}
	rest := data[1:]
	n, adv := binary.Uvarint(rest)
	if adv <= 0 || n > uint64(len(rest)-adv) {
		return nil, errors.New("grpcidem: corrupted stored payload")
	}
	name := string(rest[adv : adv+int(n)])
	body := rest[adv+int(n):]

	mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(name))
	if err != nil {
		return nil, fmt.Errorf("grpcidem: response type %q not found in the proto registry: %w", name, err)
	}
	msg := mt.New().Interface()
	if err := proto.Unmarshal(body, msg); err != nil {
		return nil, fmt.Errorf("grpcidem: unmarshaling stored response: %w", err)
	}
	return msg, nil
}
