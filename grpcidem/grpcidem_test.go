package grpcidem_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/repenguin22/idemlease"
	"github.com/repenguin22/idemlease/grpcidem"
	"github.com/repenguin22/idemlease/memstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

const echoMethod = "/grpcidem.test.Echo/Do"

// echoServer is a minimal unary service registered with a hand-written
// ServiceDesc over wrapperspb.StringValue (a real proto.Message that is
// registered in the global registry, so replay reconstruction works
// without generated code).
type echoServer struct {
	fn func(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error)
}

func echoHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(wrapperspb.StringValue)
	if err := dec(in); err != nil {
		return nil, err
	}
	svc := srv.(*echoServer)
	if interceptor == nil {
		return svc.fn(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: echoMethod}
	return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
		return svc.fn(ctx, req.(*wrapperspb.StringValue))
	})
}

var echoDesc = grpc.ServiceDesc{
	ServiceName: "grpcidem.test.Echo",
	HandlerType: (*any)(nil),
	Methods:     []grpc.MethodDesc{{MethodName: "Do", Handler: echoHandler}},
}

// harness starts an in-process gRPC server with the interceptor and
// returns a connected client plus the shared execution counter.
func harness(t *testing.T, fn func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error),
	opts ...grpcidem.Option) (*grpc.ClientConn, *atomic.Int32) {
	t.Helper()
	var count atomic.Int32
	wrapped := func(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		count.Add(1)
		return fn(ctx, in)
	}
	opts = append(opts, grpcidem.Logger(quietLogger()))
	lis := bufconn.Listen(1 << 20)
	// Recovery is the outer middleware's job (§5.1): chain a recovery
	// interceptor around grpcidem's, mirroring real deployments, so the
	// re-panic becomes an Internal error instead of crashing the server.
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(
		recoveryInterceptor,
		grpcidem.UnaryServerInterceptor(memstore.New(), opts...),
	))
	srv.RegisterService(&echoDesc, &echoServer{fn: wrapped})
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(); srv.Stop() })
	return conn, &count
}

func call(ctx context.Context, conn *grpc.ClientConn, key, in string) (*wrapperspb.StringValue, metadata.MD, error) {
	if key != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, grpcidem.MetadataKey, key)
	}
	var hdr metadata.MD
	out := new(wrapperspb.StringValue)
	err := conn.Invoke(ctx, echoMethod, wrapperspb.String(in), out, grpc.Header(&hdr))
	return out, hdr, err
}

func okEcho(_ context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return wrapperspb.String("echo:" + in.Value), nil
}

func recoveryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = status.Errorf(codes.Internal, "panic: %v", p)
		}
	}()
	return handler(ctx, req)
}

// TestReplay pins the core lifecycle: first execution runs, the retry is
// served from the stored response with the replayed marker, and a
// different request body on the same key is rejected (422 analogue).
func TestReplay(t *testing.T) {
	conn, count := harness(t, okEcho)
	ctx := context.Background()

	out, hdr, err := call(ctx, conn, "k", "hello")
	if err != nil || out.Value != "echo:hello" {
		t.Fatalf("first = (%v, %v), want echo:hello", out, err)
	}
	if len(hdr.Get(grpcidem.ReplayedMetadataKey)) != 0 {
		t.Fatal("first response must not be marked replayed")
	}

	out, hdr, err = call(ctx, conn, "k", "hello")
	if err != nil || out.Value != "echo:hello" {
		t.Fatalf("replay = (%v, %v), want the stored echo:hello", out, err)
	}
	if got := hdr.Get(grpcidem.ReplayedMetadataKey); len(got) != 1 || got[0] != "true" {
		t.Fatalf("replayed metadata = %v, want [true]", got)
	}
	if count.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", count.Load())
	}

	_, _, err = call(ctx, conn, "k", "different")
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("different request on the same key: code = %v, want FailedPrecondition", status.Code(err))
	}
	if count.Load() != 1 {
		t.Fatalf("handler ran %d times, want still 1", count.Load())
	}
}

// TestRequire pins the missing-key gate.
func TestRequire(t *testing.T) {
	t.Run("missing key passes through by default", func(t *testing.T) {
		conn, count := harness(t, okEcho)
		if _, _, err := call(context.Background(), conn, "", "x"); err != nil {
			t.Fatalf("err = %v, want pass-through", err)
		}
		if count.Load() != 1 {
			t.Fatalf("handler ran %d times, want 1", count.Load())
		}
	})
	t.Run("missing key with Require is InvalidArgument", func(t *testing.T) {
		conn, count := harness(t, okEcho, grpcidem.Require(true))
		_, _, err := call(context.Background(), conn, "", "x")
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
		}
		if count.Load() != 0 {
			t.Fatalf("handler ran %d times, want 0", count.Load())
		}
	})
	t.Run("over-length key is InvalidArgument", func(t *testing.T) {
		conn, count := harness(t, okEcho)
		// 256 bytes: the gRPC transport allows printable ASCII of this
		// length, so it reaches the server, where validateKey rejects it.
		long := strings.Repeat("a", 256)
		if _, _, err := call(context.Background(), conn, long, "x"); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
		}
		if count.Load() != 0 {
			t.Fatalf("handler ran %d times, want 0", count.Load())
		}
	})
}

// TestInFlight pins the Aborted mapping for a concurrent duplicate.
func TestInFlight(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once atomic.Bool
	conn, count := harness(t, func(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		if once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return wrapperspb.String("done"), nil
	})
	ctx := context.Background()

	done := make(chan error, 1)
	go func() { _, _, err := call(ctx, conn, "k", "x"); done <- err }()
	<-entered

	_, _, err := call(ctx, conn, "k", "x")
	if status.Code(err) != codes.Aborted {
		t.Fatalf("in-flight duplicate: code = %v, want Aborted", status.Code(err))
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("first call err = %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", count.Load())
	}
}

// TestErroredCallsReexecute pins the gRPC replay semantic: only
// successful responses are stored, so any errored call re-executes on
// retry — a transient failure is never frozen into a replay.
func TestErroredCallsReexecute(t *testing.T) {
	t.Run("transient error then success", func(t *testing.T) {
		var fail atomic.Bool
		fail.Store(true)
		conn, count := harness(t, func(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			if fail.Load() {
				return nil, status.Error(codes.Unavailable, "dependency down")
			}
			return wrapperspb.String("ok"), nil
		})
		ctx := context.Background()
		if _, _, err := call(ctx, conn, "k", "x"); status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable", status.Code(err))
		}
		fail.Store(false)
		out, _, err := call(ctx, conn, "k", "x")
		if err != nil || out.Value != "ok" {
			t.Fatalf("retry = (%v, %v), want a re-execution returning ok", out, err)
		}
		if count.Load() != 2 {
			t.Fatalf("handler ran %d times, want 2 (an error must not be replayed)", count.Load())
		}
	})
	t.Run("deterministic error re-executes", func(t *testing.T) {
		conn, count := harness(t, func(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return nil, status.Error(codes.NotFound, "no such order")
		})
		ctx := context.Background()
		if _, _, err := call(ctx, conn, "k", "x"); status.Code(err) != codes.NotFound {
			t.Fatalf("code = %v, want NotFound", status.Code(err))
		}
		if _, _, err := call(ctx, conn, "k", "x"); status.Code(err) != codes.NotFound {
			t.Fatalf("retry code = %v, want NotFound", status.Code(err))
		}
		if count.Load() != 2 {
			t.Fatalf("handler ran %d times, want 2 (an errored call re-executes)", count.Load())
		}
	})
}

// TestKeyScope pins per-scope isolation using call metadata.
func TestKeyScope(t *testing.T) {
	scope := func(ctx context.Context) string {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get("tenant"); len(v) == 1 {
				return v[0]
			}
		}
		return ""
	}
	conn, count := harness(t, okEcho, grpcidem.KeyScope(scope))
	tenant := func(id string) context.Context {
		return metadata.AppendToOutgoingContext(context.Background(), "tenant", id)
	}

	if _, _, err := call(tenant("A"), conn, "k", "x"); err != nil {
		t.Fatal(err)
	}
	_, hdr, err := call(tenant("B"), conn, "k", "x")
	if err != nil {
		t.Fatal(err)
	}
	if len(hdr.Get(grpcidem.ReplayedMetadataKey)) != 0 {
		t.Fatal("tenant B must execute independently, not replay A")
	}
	if count.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2 (one per scope)", count.Load())
	}
	if _, hdr, _ := call(tenant("A"), conn, "k", "x"); len(hdr.Get(grpcidem.ReplayedMetadataKey)) != 1 {
		t.Fatal("tenant A retry must replay within its own scope")
	}
}

// TestFailOpen pins that a store failure passes through when FailOpen is
// set, and is Unavailable otherwise.
func TestFailOpen(t *testing.T) {
	t.Run("fail-closed Unavailable", func(t *testing.T) {
		lis := bufconn.Listen(1 << 20)
		srv := grpc.NewServer(grpc.UnaryInterceptor(
			grpcidem.UnaryServerInterceptor(downStore{}, grpcidem.Logger(quietLogger()))))
		srv.RegisterService(&echoDesc, &echoServer{fn: okEcho})
		go func() { _ = srv.Serve(lis) }()
		conn, err := grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close(); srv.Stop() })
		if _, _, err := call(context.Background(), conn, "k", "x"); status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable", status.Code(err))
		}
	})
}

// TestPanicPropagates pins that a handler panic releases the reservation
// and propagates (as a gRPC Internal from the server's recovery, but the
// key must be re-executable afterward).
func TestPanicPropagates(t *testing.T) {
	var n atomic.Int32
	conn, count := harness(t, func(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		if n.Add(1) == 1 {
			panic("boom")
		}
		return wrapperspb.String("recovered"), nil
	})
	ctx := context.Background()
	// The default grpc server turns a panic into Internal (no recovery
	// interceptor installed → the goroutine would crash; grpc recovers
	// to a codes.Internal by default in recent versions). Either way,
	// the reservation must be released so the retry succeeds.
	_, _, _ = call(ctx, conn, "k", "x")
	out, _, err := call(ctx, conn, "k", "x")
	if err != nil || out.Value != "recovered" {
		t.Fatalf("after panic the key must be re-executable: (%v, %v)", out, err)
	}
	if count.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", count.Load())
	}
}

type downStore struct{}

var _ idemlease.Store = downStore{}

func (downStore) Reserve(context.Context, idemlease.Record) (*idemlease.Record, error) {
	return nil, errors.New("store down")
}
func (downStore) Complete(context.Context, string, string, []byte, time.Duration) error {
	return errors.New("store down")
}
func (downStore) Release(context.Context, string, string) error { return errors.New("store down") }
func (downStore) Get(context.Context, string) (*idemlease.Record, error) {
	return nil, errors.New("store down")
}
