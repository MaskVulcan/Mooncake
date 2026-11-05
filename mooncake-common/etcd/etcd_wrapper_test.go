package main

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func TestKeepAliveReadyRequiresPositiveServerAcknowledgement(t *testing.T) {
	const lease int64 = -991
	ctx, cancel := context.WithCancel(context.Background())
	storeKeepAliveMutex.Lock()
	state := &leaseKeepAliveState{ctx: ctx, cancel: cancel}
	storeKeepAliveCtx[lease] = state
	storeKeepAliveMutex.Unlock()
	defer cancelAndDeleteKeepAlive(lease)
	if hasKeepAliveAcknowledgement(lease) {
		t.Fatal("a locally created context is not a lease acknowledgement")
	}
	if acknowledgeKeepAlive(lease, state, &clientv3.LeaseKeepAliveResponse{ID: clientv3.LeaseID(lease), TTL: 0}) || acknowledgeKeepAlive(lease, state, &clientv3.LeaseKeepAliveResponse{ID: clientv3.LeaseID(lease), TTL: -1}) {
		t.Fatal("expired leases must not become ready")
	}
	if !acknowledgeKeepAlive(lease, state, &clientv3.LeaseKeepAliveResponse{ID: clientv3.LeaseID(lease), TTL: 15}) || !hasKeepAliveAcknowledgement(lease) {
		t.Fatal("positive server acknowledgement did not make lease ready")
	}
	cancelAndDeleteKeepAlive(lease)
	if ctx.Err() == nil || hasKeepAliveAcknowledgement(lease) || acknowledgeKeepAlive(lease, state, &clientv3.LeaseKeepAliveResponse{ID: clientv3.LeaseID(lease), TTL: 15}) {
		t.Fatal("cancelled lease must not remain or become ready")
	}
}

func TestNewMaintenanceSessionCancelsStartup(t *testing.T) {
	original := startMaintenanceSession
	defer func() { startMaintenanceSession = original }()

	started := make(chan struct{})
	startMaintenanceSession = func(ctx context.Context, _ *clientv3.Client,
		_ int) (*concurrency.Session, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	result := make(chan error, 1)
	go func() {
		_, err := newMaintenanceSession(nil, 30, 10*time.Millisecond)
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("maintenance session startup was not attempted")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected startup cancellation error")
		}
	case <-time.After(time.Second):
		t.Fatal("maintenance session startup did not cancel")
	}
}

func TestKeepAliveReadinessRejectsWrongLeaseAndWaitsForAcknowledgement(t *testing.T) {
	const lease int64 = -992
	ctx, cancel := context.WithCancel(context.Background())
	storeKeepAliveMutex.Lock()
	state := &leaseKeepAliveState{ctx: ctx, cancel: cancel}
	storeKeepAliveCtx[lease] = state
	storeKeepAliveMutex.Unlock()
	t.Cleanup(func() { cancelAndDeleteKeepAlive(lease) })

	if err := waitKeepAliveReady(lease, 0); err == nil {
		t.Fatal("local context must not satisfy readiness")
	}
	for _, response := range []*clientv3.LeaseKeepAliveResponse{
		nil, {ID: clientv3.LeaseID(lease + 1), TTL: 15},
		{ID: clientv3.LeaseID(lease), TTL: 0},
	} {
		if acknowledgeKeepAlive(lease, state, response) {
			t.Fatal("invalid response made the lease ready")
		}
	}
	if err := waitKeepAliveReady(lease, 2*time.Millisecond); err == nil {
		t.Fatal("readiness must time out without a valid response")
	}
	result := make(chan error, 1)
	go func() { result <- waitKeepAliveReady(lease, time.Second) }()
	if !acknowledgeKeepAlive(lease, state, &clientv3.LeaseKeepAliveResponse{
		ID: clientv3.LeaseID(lease), TTL: 15,
	}) {
		t.Fatal("valid response rejected after a readiness timeout")
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("readiness did not observe the acknowledgment")
	}
	cancelAndDeleteKeepAlive(lease)
	if err := waitKeepAliveReady(lease, 0); err == nil {
		t.Fatal("cancellation must clear readiness")
	}
}

func TestRetiredKeepAliveCannotAcknowledgeOrDeleteReplacement(t *testing.T) {
	const lease int64 = -993
	oldCtx, oldCancel := context.WithCancel(context.Background())
	old := &leaseKeepAliveState{ctx: oldCtx, cancel: oldCancel}
	storeKeepAliveMutex.Lock()
	storeKeepAliveCtx[lease] = old
	storeKeepAliveMutex.Unlock()
	cancelAndDeleteKeepAlive(lease)

	ctx, cancel := context.WithCancel(context.Background())
	current := &leaseKeepAliveState{ctx: ctx, cancel: cancel}
	storeKeepAliveMutex.Lock()
	storeKeepAliveCtx[lease] = current
	storeKeepAliveMutex.Unlock()
	t.Cleanup(func() { cancelAndDeleteKeepAlive(lease) })
	response := &clientv3.LeaseKeepAliveResponse{ID: clientv3.LeaseID(lease), TTL: 15}
	if acknowledgeKeepAlive(lease, old, response) || hasKeepAliveAcknowledgement(lease) {
		t.Fatal("retired stream acknowledged the replacement")
	}
	finishKeepAlive(lease, old)
	if ctx.Err() != nil || !acknowledgeKeepAlive(lease, current, response) {
		t.Fatal("retired stream cleanup cancelled the replacement")
	}
	cancel()
	if hasKeepAliveAcknowledgement(lease) || acknowledgeKeepAlive(lease, current, response) {
		t.Fatal("cancelled context retained readiness")
	}
}
