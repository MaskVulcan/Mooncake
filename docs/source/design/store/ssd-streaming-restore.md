---
orphan: true
---

# Bounded SSD streaming restore

SSD streaming restores an object through a bounded staging arena instead of
allocating space for the entire object. The bucket file format, master disk
replica descriptors, checksum checks, and existing restore APIs stay compatible.
The feature is opt-in.

## Supported paths

| Path | Destination | Enablement |
| --- | --- | --- |
| Same-process streaming | CPU slices | `MOONCAKE_OFFLOAD_LOCAL_STREAMING_READ=true` |
| Remote Owner push | Registered CPU pool over classic TCP or RDMA | `MOONCAKE_OFFLOAD_SSD_PUSH=true` and trusted peers |
| Existing restore | Existing supported CPU/device paths | Default and safe pre-submission fallback |

Direct device destinations and TENT SSD push are unsupported. TENT Store reads
retain their native scheduling and transfer intent; their completion now also
requires a physical drain check. Running on an accelerator server does not
establish device-memory coverage.

## Owner pipeline

The bucket backend acquires a read guard and owned file descriptor for each
immutable object range. A checked plan maps the source range to destination
fragments. Bounded slots advance through read, ready, write and drained states.
A slot cannot be reused until its read and destination writes have completed.
The source guards remain held through that same barrier.

The portable driver uses buffered `pread`. With liburing, the native driver uses
registered files and aligned registered buffers with `READ_FIXED`. Each Owner
batch creates one ring and buffer registration. Subsequent objects reuse them
only after the previous pipeline and callbacks have drained; a fixed-file update
must acknowledge exactly one updated slot. Failed updates poison further
submissions and keep the prior source alive until teardown. Rings are scoped to
one batch, rather than pooled across requests.

## Fixed CPU destination pools

Register a CPU pool once with the existing `register_buffer` API. SSD push
imports its address and registered bounds into a destination registry. Grants
bind a write to the requester identity, boot epoch, registration generation,
range and attempt. Owner validation rejects stale or mismatched grants before
posting transfers. The MR and backing storage remain alive until successful
unregister or close.

For an admitted single-Owner batch, the normal path uses one terminal `ReadInto`
control RPC. The Owner streams SSD bytes directly into the granted pool and
reports drained completion. Master lookup and Transfer Engine metadata or
connection traffic are separate from that count. The explicit V1 components
also support per-read destinations and Submit/Query/Forget; that protocol is
separate from the fixed-pool normal path.

Classic TCP uses acknowledged v2 WRITE framing. RDMA uses RC completion for
successful writes. A transport error, elapsed timeout, or peer restart alone
does not prove that the destination is safe to reuse.

## Buffer lifetime and deadlines

Public synchronous Store reads retain submitted work until it can no longer
access caller buffers, including request descriptors and completion callback
tails. Status polling determines the logical result; a separate physical drain
barrier determines when ownership may be returned. This applies to classic
Transfer Engine and native TENT batches, including submission exceptions.

An optional positive `timeout_ms` on RealClient `batch_get_into` and
`batch_get_into_multi_buffers` is one logical budget spanning metadata lookup,
memory transfers, SSD reads and Owner groups. Expiry stops new submissions and
prevents a late success result. Accepted work drains before return, so the call
can outlast the budget. Zero does not establish a shared batch deadline.
Individual Transfer Engine waits use `MC_TRANSFER_TIMEOUT_SEC` (60 seconds by
default); invalid or non-positive environment values use the default. Native
scatter preserves the existing `MC_TENT_TRANSFER_TIMEOUT_S` override when set.
Direct SSD restores use their runtime default budget. DummyClient's IPC protocol
does not propagate
this budget and rejects a positive explicit timeout before submission.

Low-level scatter `waitFor` may return a timeout while its operation still owns
the batch. Keep the operation alive and call `wait`, or destroy it to drain.
Callers must retain exclusive access to destination memory throughout that
ownership period.

For an uncertain direct write, the synchronous runtime closes the initial RPC at
its deadline and uses bounded recovery RPCs until it confirms terminal drain for
the same attempt. A permanent fault without an authenticated remote fence can
retain the worker and destination indefinitely. A failed completion or a fixed
grace interval does not permit reuse or a legacy retry. Advanced requester APIs
expose explicit recovery for callers that manage their own operation lifetime.

Busy unregister or close returns `OBJECT_HAS_LEASE` (`-706`). Keep external
buffers and the Store alive until cleanup succeeds. Retry
`unregister_buffer` for incomplete registration cleanup; `recover_ssd_push`
recovers read attempts, not uncertain registration operations.

## Trusted peer setup

Peer JSON returned by `get_ssd_push_peer_info` contains setup credentials.
Exchange it through a trusted channel and configure reciprocal peers with
`configure_ssd_push_peers`. Do not publish these credentials in public Transfer
Engine metadata.

Alternatively, set `MOONCAKE_OFFLOAD_SSD_PEER_DIRECTORY` to a deployment-owned
private directory shared by participating processes. Use the same service UID,
directory mode `0700`, and record mode `0600`. Each runtime atomically publishes
its exact identity and boot epoch and removes only its own record after safe
close. Discovery accepts at most 64 records and rejects insecure files,
symlinks and duplicate routes. The deployment must remove stale records from
crashed processes before reusing their endpoint, while preserving records needed
by still-active attempts. This option does not add master-based discovery.

## Configuration and fallback

| Variable | Default | Meaning |
| --- | --- | --- |
| `MOONCAKE_OFFLOAD_LOCAL_STREAMING_READ` | `false` | Same-process CPU streaming |
| `MOONCAKE_OFFLOAD_SSD_PUSH` | `false` | Fixed CPU pool restore with trusted peers |
| `MOONCAKE_OFFLOAD_STREAM_USE_URING` | `true` with liburing and SSD push, otherwise `false` | Native streaming driver; independent of legacy `MOONCAKE_OFFLOAD_USE_URING` |
| `MOONCAKE_OFFLOAD_STREAM_SLOT_BYTES` | `1048576` | 4 KiB multiple, 4 KiB–64 MiB |
| `MOONCAKE_OFFLOAD_STREAM_SLOTS` | `4` | Slots per active batch, 1–64 |
| `MOONCAKE_OFFLOAD_STREAM_READ_THREADS` | `4` | Shared read workers, 1–64 |
| `MOONCAKE_OFFLOAD_STREAM_WRITE_THREADS` | `2` | Shared copy workers, 1–64 |
| `MOONCAKE_OFFLOAD_STREAM_MAX_OUTSTANDING` | `512` with SSD push, otherwise `64` | Shared task and source admission limit |
| `MOONCAKE_OFFLOAD_READ_CONCURRENCY` | `1` | Owner endpoint groups per read, bounded to 8 |
| `MOONCAKE_OFFLOAD_READ_IO_THREADS` | `64` | Dedicated SSD RPC executor threads, 1–128 |

The staging allocator needs `slots * (slot_bytes + 4095)` bytes. Outstanding
capacity must be at least twice the slot count and at most 65,536. Every batch
reserves source credits before opening files. Direct runtime defaults allow 512
objects per Owner batch and 65,536 fixed pool registrations; old advertisements
without `max_batch_objects` retain their 64-object limit.

Unsupported routes and admission failures before any destination write can
use the existing restore path. A validated drained terminal rejection with zero
written bytes may also fall back. Accepted or uncertain writes require recovery
of the original attempt. Source/destination ranges are checked for overflow,
capacity, overlap and staging-arena aliasing before submission. An I/O failure
may leave partial data; publish data only on whole-batch success.

## Validation

The [test guide](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store/tests/ssd_offload/README.md)
describes portable state-machine tests, native io_uring tests, local Store and
Python integration, and separate hardware smoke tests. Tests cover bounded
arenas, ranges, canaries, source deletion guards, deadlines, callback tails,
trusted setup, stale grants, recovery, pool reuse and batch ring reuse.

Portable tests do not prove kernel CQE execution. TCP tests do not prove RNIC
behavior. Hardware validation must record its build, driver, destination memory
type and actual protocol separately. No throughput improvement or device DMA
coverage is asserted by this design.
