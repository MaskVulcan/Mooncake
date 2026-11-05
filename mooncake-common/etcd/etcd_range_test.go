package main

import (
	"context"
	"encoding/json"
	"net"
	"reflect"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type rangeTestServer struct {
	pb.UnimplementedKVServer
	rangeFn  func(*pb.RangeRequest) (*pb.RangeResponse, error)
	deleteFn func(*pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error)
}

func (s *rangeTestServer) Range(_ context.Context, req *pb.RangeRequest) (*pb.RangeResponse, error) {
	return s.rangeFn(req)
}

func (s *rangeTestServer) DeleteRange(_ context.Context, req *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	return s.deleteFn(req)
}

func newRangeTestClient(t *testing.T, srv *rangeTestServer) *clientv3.Client {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	pb.RegisterKVServer(server, srv)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"passthrough:///range-test"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{grpc.WithContextDialer(
			func(context.Context, string) (net.Conn, error) { return listener.Dial() })},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestRangeKeysSendsBoundedKeysOnlyRequest(t *testing.T) {
	requests := make(chan *pb.RangeRequest, 1)
	client := newRangeTestClient(t, &rangeTestServer{rangeFn: func(req *pb.RangeRequest) (*pb.RangeResponse, error) {
		requests <- req
		return &pb.RangeResponse{Kvs: []*mvccpb.KeyValue{
			{Key: []byte("batch/001")}, {Key: []byte("batch/009")},
		}}, nil
	}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := rangeKeysJSON(ctx, client, "batch/000", "batch/100", 2)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	if err := json.Unmarshal(got, &keys); err != nil || !reflect.DeepEqual(keys, []string{"batch/001", "batch/009"}) {
		t.Fatalf("unexpected keys %s, error %v", got, err)
	}
	req := <-requests
	if string(req.Key) != "batch/000" || string(req.RangeEnd) != "batch/100" ||
		req.Limit != 2 || !req.KeysOnly || req.CountOnly || req.Serializable ||
		req.SortTarget != pb.RangeRequest_KEY ||
		(req.SortOrder != pb.RangeRequest_ASCEND && req.SortOrder != pb.RangeRequest_NONE) {
		t.Fatalf("unsafe range request: %+v", req)
	}
}

func TestRangeKeysRejectsInvalidBoundsAndLimitsWithoutRPC(t *testing.T) {
	for _, tc := range []struct {
		start, end string
		limit      int64
	}{
		{"", "z", 1}, {"a", "", 1}, {"a", "\x00", 1}, {"z", "a", 1},
		{"a", "z", 0}, {"a", "z", -1}, {"a", "z", 1 << 31},
	} {
		if _, err := rangeKeysJSON(context.Background(), nil, tc.start, tc.end, tc.limit); err == nil {
			t.Fatalf("accepted invalid range %+v", tc)
		}
	}
	got, err := rangeKeysJSON(context.Background(), nil, "a", "a", 1)
	if err != nil || string(got) != "[]" {
		t.Fatalf("equal range: got %s, error %v", got, err)
	}
}

func TestRangeKeysRejectsMalformedResponses(t *testing.T) {
	for name, keys := range map[string][]*mvccpb.KeyValue{
		"out_of_range": {{Key: []byte("z")}},
		"unordered":    {{Key: []byte("b")}, {Key: []byte("a")}},
		"duplicate":    {{Key: []byte("b")}, {Key: []byte("b")}},
		"oversize":     {{Key: []byte("b")}, {Key: []byte("c")}, {Key: []byte("d")}},
		"invalid_utf8": {{Key: []byte{'b', 0xff}}},
	} {
		t.Run(name, func(t *testing.T) {
			client := newRangeTestClient(t, &rangeTestServer{rangeFn: func(*pb.RangeRequest) (*pb.RangeResponse, error) {
				return &pb.RangeResponse{Kvs: keys}, nil
			}})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if got, err := rangeKeysJSON(ctx, client, "a", "z", 2); err == nil || got != nil {
				t.Fatalf("accepted malformed response %s, error %v", got, err)
			}
		})
	}
}

func TestDeleteRangeCountedUsesActualCountAndFiniteBounds(t *testing.T) {
	requests := make(chan *pb.DeleteRangeRequest, 1)
	client := newRangeTestClient(t, &rangeTestServer{deleteFn: func(req *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
		requests <- req
		return &pb.DeleteRangeResponse{Deleted: 7}, nil
	}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	count, err := deleteRangeCounted(ctx, client, "batch/000", "batch/100")
	if err != nil || count != 7 {
		t.Fatalf("delete count %d, error %v", count, err)
	}
	req := <-requests
	if string(req.Key) != "batch/000" || string(req.RangeEnd) != "batch/100" || req.PrevKv {
		t.Fatalf("unexpected delete request: %+v", req)
	}
	for _, bounds := range [][2]string{{"", "z"}, {"a", ""}, {"a", "\x00"}, {"z", "a"}} {
		if count, err := deleteRangeCounted(context.Background(), nil, bounds[0], bounds[1]); err == nil || count != 0 {
			t.Fatalf("accepted bounds %q: count %d, error %v", bounds, count, err)
		}
	}
	if count, err := deleteRangeCounted(context.Background(), nil, "a", "a"); err != nil || count != 0 {
		t.Fatalf("equal range: count %d, error %v", count, err)
	}
}

func TestCountedRangeErrorsDoNotReturnProgress(t *testing.T) {
	client := newRangeTestClient(t, &rangeTestServer{
		rangeFn: func(*pb.RangeRequest) (*pb.RangeResponse, error) {
			return nil, status.Error(codes.InvalidArgument, "read rejected")
		},
		deleteFn: func(*pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
			return nil, status.Error(codes.InvalidArgument, "delete rejected")
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if got, err := rangeKeysJSON(ctx, client, "a", "z", 1); err == nil || got != nil {
		t.Fatalf("read returned %s, error %v", got, err)
	}
	if count, err := deleteRangeCounted(ctx, client, "a", "z"); err == nil || count != 0 {
		t.Fatalf("delete returned %d, error %v", count, err)
	}
}
