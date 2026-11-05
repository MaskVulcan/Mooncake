#include "ha/snapshot/batch_oplog/batch_oplog_pruning_coordinator.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <glog/logging.h>
#include <limits>
#include <thread>
#include <utility>

#include "crc32c.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_types.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "ha/snapshot/snapshot_maintenance_lease.h"

#include "ha_metric_manager.h"

namespace mooncake {
namespace {

constexpr size_t kMaxRequestsPerPass = 16;
constexpr auto kMaxPassTime = std::chrono::seconds(1);

bool ParseFloor(const std::string& value, uint64_t& floor) {
    const auto parsed =
        std::from_chars(value.data(), value.data() + value.size(), floor);
    return !value.empty() && parsed.ec == std::errc() &&
           parsed.ptr == value.data() + value.size();
}

bool ParseBatchKey(const std::string& key, const std::string& prefix,
                   uint64_t& batch_id) {
    if (!key.starts_with(prefix) ||
        key.size() != prefix.size() + kOpLogBatchIdWidth)
        return false;
    const auto* begin = key.data() + prefix.size();
    const auto* end = key.data() + key.size();
    if (!std::all_of(begin, end, [](char c) { return c >= '0' && c <= '9'; }))
        return false;
    const auto parsed = std::from_chars(begin, end, batch_id);
    return parsed.ec == std::errc() && parsed.ptr == end;
}

bool InspectArtifact(SnapshotObjectStore& store, const std::string& key,
                     uint64_t size, uint32_t crc) {
    const auto inspection = store.InspectObject(key);
    // A successful inspection with no checksum means the backend does not
    // supply one. N03's full pre-publication readback remains the content
    // evidence; do not download all chunks again here. Unknown metadata is an
    // inspection error and fails closed.
    return size != 0 && inspection && inspection->stored_size == size &&
           (!inspection->crc32c || *inspection->crc32c == crc);
}

bool ValidateSnapshot(SnapshotObjectStore& store, const std::string& root,
                      const std::string& pointer,
                      const ha::BatchOpLogSnapshotDescriptor& descriptor,
                      const std::function<bool()>& cancelled) {
    if (descriptor.manifest_key !=
        ha::BuildBatchOpLogSnapshotManifestKey(root, descriptor.snapshot_id))
        return false;
    std::string bytes;
    const auto descriptor_key =
        ha::BuildBatchOpLogSnapshotDescriptorKey(root, descriptor.snapshot_id);
    if (descriptor_key.empty() ||
        !store.DownloadString(descriptor_key, bytes) || bytes != pointer)
        return false;
    if (!store.DownloadString(descriptor.manifest_key, bytes) ||
        bytes.size() != descriptor.manifest_size ||
        Crc32cValue(bytes.data(), bytes.size()) != descriptor.manifest_crc32c)
        return false;
    const auto manifest = ha::DecodeBatchOpLogSnapshotManifest(bytes);
    if (!manifest || manifest->schema_version != descriptor.schema_version ||
        manifest->snapshot_format != descriptor.snapshot_format ||
        manifest->snapshot_id != descriptor.snapshot_id ||
        manifest->last_included_batch_id != descriptor.last_included_batch_id ||
        manifest->last_included_seq != descriptor.last_included_seq ||
        manifest->producer_view_version != descriptor.producer_view_version ||
        manifest->segments.key != ha::BuildBatchOpLogSnapshotSegmentsKey(
                                      root, descriptor.snapshot_id) ||
        !InspectArtifact(store, manifest->segments.key,
                         manifest->segments.stored_size,
                         manifest->segments.crc32c))
        return false;
    for (size_t i = 0; i < manifest->object_chunks.size(); ++i) {
        if (cancelled()) return false;
        const auto& chunk = manifest->object_chunks[i];
        if (chunk.chunk_index != i ||
            chunk.key != ha::BuildBatchOpLogSnapshotObjectChunkKey(
                             root, descriptor.snapshot_id, i) ||
            !InspectArtifact(store, chunk.key, chunk.stored_size, chunk.crc32c))
            return false;
    }
    if (manifest->weight_metadata) {
        const auto& weights = *manifest->weight_metadata;
        if (weights.key != ha::BuildBatchOpLogSnapshotWeightMetadataKey(
                               root, descriptor.snapshot_id) ||
            !InspectArtifact(store, weights.key, weights.stored_size,
                             weights.crc32c))
            return false;
    }
    return true;
}

}  // namespace

BatchOpLogPruningCoordinator::BatchOpLogPruningCoordinator(
    HaKvBackend& backend, SnapshotObjectStore& object_store,
    std::string cluster_id, std::string snapshot_root,
    BatchOpLogPruningConfig config)
    : backend_(backend),
      object_store_(object_store),
      cluster_id_(std::move(cluster_id)),
      snapshot_root_(std::move(snapshot_root)),
      config_(config) {}

bool BatchOpLogPruningCoordinator::ValidContext(
    const SnapshotMaintenanceLease& lease) const {
    const auto lock_key =
        ha::BuildBatchOpLogSnapshotMaintenanceKey(cluster_id_);
    return !lock_key.empty() && lease.lock_key() == lock_key &&
           config_.max_batches_per_request != 0 &&
           config_.max_batches_per_request <=
               static_cast<size_t>(std::numeric_limits<int>::max()) &&
           config_.batch_interval_ms <= std::numeric_limits<uint32_t>::max();
}

ErrorCode BatchOpLogPruningCoordinator::Run(
    const SnapshotMaintenanceLease& lease,
    const std::function<bool()>& cancelled, bool* more_work) {
    if (more_work) *more_work = false;
    HAMetricManager::SnapshotOperationTimer metric_timer(
        HAMetricManager::SnapshotOperation::Prune);
    const auto latest_key = ha::BuildBatchOpLogSnapshotLatestKey(cluster_id_);
    const auto fallback_key =
        ha::BuildBatchOpLogSnapshotFallbackKey(cluster_id_);
    const auto floor_key =
        ha::BuildBatchOpLogSnapshotCompactionFloorKey(cluster_id_);
    const auto lock_key =
        ha::BuildBatchOpLogSnapshotMaintenanceKey(cluster_id_);
    if (!ValidContext(lease) || snapshot_root_.empty() ||
        !backend_.SupportsTxn())
        return ErrorCode::INVALID_PARAMS;
    const auto stopped = [&] {
        return !lease.IsHeld() || (cancelled && cancelled());
    };
    if (stopped()) return ErrorCode::ETCD_TRANSACTION_FAIL;

    HAMetricManager::instance().update_snapshot_runtime(
        [](auto& metrics) { metrics.candidate_floor = 0; });
    std::string latest, fallback, old_floor;
    auto error = backend_.Get(latest_key, latest);
    if (error != ErrorCode::OK) return error;
    error = backend_.Get(fallback_key, fallback);
    // The first publication has no redundant recovery baseline yet.
    if (error == ErrorCode::ETCD_KEY_NOT_EXIST) {
        HAMetricManager::instance().record_snapshot_skip(
            HAMetricManager::SnapshotSkipReason::NoFallback);
        return metric_timer.Success(ErrorCode::OK);
    }
    if (error != ErrorCode::OK) return error;
    const auto latest_descriptor =
        ha::DecodeBatchOpLogSnapshotDescriptor(latest);
    const auto fallback_descriptor =
        ha::DecodeBatchOpLogSnapshotDescriptor(fallback);
    if (!latest_descriptor || !fallback_descriptor)
        return ErrorCode::INTERNAL_ERROR;
    const uint64_t candidate = fallback_descriptor->last_included_batch_id;
    if (latest_descriptor->snapshot_id == fallback_descriptor->snapshot_id ||
        latest_descriptor->last_included_batch_id <= candidate) {
        HAMetricManager::instance().record_snapshot_skip(
            HAMetricManager::SnapshotSkipReason::InvalidPair);
        return metric_timer.Success(ErrorCode::OK);
    }

    HAMetricManager::instance().update_snapshot_runtime(
        [&](auto& metrics) { metrics.candidate_floor = candidate; });
    uint64_t floor = 0;
    error = backend_.Get(floor_key, old_floor);
    const bool floor_exists = error == ErrorCode::OK;
    if (!floor_exists && error != ErrorCode::ETCD_KEY_NOT_EXIST) return error;
    if (floor_exists) {
        if (!ParseFloor(old_floor, floor)) return ErrorCode::INTERNAL_ERROR;
        if (more_work) *more_work = true;
        HAMetricManager::instance().update_snapshot_runtime([&](auto& metrics) {
            metrics.compaction_floor =
                std::max(metrics.compaction_floor, floor);
        });
        if (candidate < floor) {
            HAMetricManager::instance().record_snapshot_skip(
                HAMetricManager::SnapshotSkipReason::FloorAhead);
            return metric_timer.Success(ErrorCode::OK);
        }
    }

    if (!ValidateSnapshot(object_store_, snapshot_root_, latest,
                          *latest_descriptor, stopped) ||
        !ValidateSnapshot(object_store_, snapshot_root_, fallback,
                          *fallback_descriptor, stopped))
        return ErrorCode::INTERNAL_ERROR;
    if (stopped()) return ErrorCode::ETCD_TRANSACTION_FAIL;

    KvTxn txn;
    txn.compares = {{.key = lock_key,
                     .kind = KvCompareKind::kValueEquals,
                     .expected_value = lease.owner_token()},
                    {.key = lock_key,
                     .kind = KvCompareKind::kCreateRevisionEquals,
                     .expected_value = "",
                     .expected_revision = lease.lock_create_revision()},
                    {.key = latest_key,
                     .kind = KvCompareKind::kValueEquals,
                     .expected_value = latest},
                    {.key = fallback_key,
                     .kind = KvCompareKind::kValueEquals,
                     .expected_value = fallback},
                    {.key = floor_key,
                     .kind = floor_exists ? KvCompareKind::kValueEquals
                                          : KvCompareKind::kKeyNotExists,
                     .expected_value = old_floor}};
    txn.puts.push_back({.key = floor_key, .value = std::to_string(candidate)});
    error = backend_.Txn(txn);
    if (error != ErrorCode::OK) return error;
    HAMetricManager::instance().update_snapshot_runtime([&](auto& metrics) {
        metrics.compaction_floor =
            std::max(metrics.compaction_floor, candidate);
        metrics.floor_advances_total += candidate > floor;
    });
    // The floor is the reader's recovery boundary, not evidence that every
    // physical key has been removed. Resume can finish this prefix without
    // another snapshot publication after a partial pass, failure or crash.
    const auto delete_error = Drain(candidate, lease, cancelled, more_work);
    metric_timer.error = static_cast<int64_t>(delete_error);
    return delete_error;
}

ErrorCode BatchOpLogPruningCoordinator::Resume(
    const SnapshotMaintenanceLease& lease,
    const std::function<bool()>& cancelled, bool* more_work) {
    if (more_work) *more_work = false;
    HAMetricManager::SnapshotOperationTimer metric_timer(
        HAMetricManager::SnapshotOperation::Prune);
    if (!ValidContext(lease)) return ErrorCode::INVALID_PARAMS;
    if (!lease.IsHeld() || (cancelled && cancelled()))
        return ErrorCode::ETCD_TRANSACTION_FAIL;
    std::string value;
    const auto error = backend_.Get(
        ha::BuildBatchOpLogSnapshotCompactionFloorKey(cluster_id_), value);
    if (error == ErrorCode::ETCD_KEY_NOT_EXIST)
        return metric_timer.Success(ErrorCode::OK);
    if (error != ErrorCode::OK) return error;
    uint64_t floor = 0;
    if (!ParseFloor(value, floor)) return ErrorCode::INTERNAL_ERROR;
    HAMetricManager::instance().update_snapshot_runtime([&](auto& metrics) {
        metrics.compaction_floor = std::max(metrics.compaction_floor, floor);
    });
    const auto delete_error = Drain(floor, lease, cancelled, more_work);
    metric_timer.error = static_cast<int64_t>(delete_error);
    return delete_error;
}

ErrorCode BatchOpLogPruningCoordinator::Drain(
    uint64_t floor, const SnapshotMaintenanceLease& lease,
    const std::function<bool()>& cancelled, bool* more_work) {
    if (more_work) *more_work = true;
    const auto stopped = [&] {
        return !lease.IsHeld() || (cancelled && cancelled());
    };
    const auto begin = BuildBatchRecordKey(cluster_id_, 0);
    const auto end = BuildBatchRecordRange(cluster_id_, floor).begin_key;
    if (begin.empty() || end.empty() || begin >= end)
        return ErrorCode::INVALID_PARAMS;
    const auto prefix = begin.substr(0, begin.size() - kOpLogBatchIdWidth);
    const auto deadline = std::chrono::steady_clock::now() + kMaxPassTime;
    for (size_t request = 0; request < kMaxRequestsPerPass; ++request) {
        if (stopped() || std::chrono::steady_clock::now() >= deadline)
            return ErrorCode::OK;
        std::vector<std::string> keys;
        auto error = backend_.RangeKeys(begin, end,
                                        config_.max_batches_per_request, keys);
        if (error != ErrorCode::OK) return error;
        if (keys.empty()) {
            if (more_work) *more_work = false;
            return ErrorCode::OK;
        }
        if (keys.size() > config_.max_batches_per_request)
            return ErrorCode::INTERNAL_ERROR;
        uint64_t last_batch_id = 0;
        for (size_t i = 0; i < keys.size(); ++i) {
            uint64_t batch_id = 0;
            if (!ParseBatchKey(keys[i], prefix, batch_id) || batch_id > floor ||
                keys[i] < begin || keys[i] >= end ||
                (i != 0 && keys[i - 1] >= keys[i]))
                return ErrorCode::INTERNAL_ERROR;
            last_batch_id = batch_id;
        }
        const auto chunk_end =
            BuildBatchRecordRange(cluster_id_, last_batch_id).begin_key;
        // Permit the first deletion even when its boundary lookup used the
        // pass budget. Otherwise consistently slow reads could prevent any
        // cleanup progress. Cancellation and lease ownership still gate it.
        if (stopped() ||
            (request != 0 && std::chrono::steady_clock::now() >= deadline))
            return ErrorCode::OK;
        uint64_t deleted = 0;
        const auto started = std::chrono::steady_clock::now();
        error = backend_.DeleteRangeCounted(keys.front(), chunk_end, deleted);
        const auto elapsed_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - started)
                .count();
        LOG(INFO) << "[SnapshotPrune] chunk cluster=" << cluster_id_
                  << " floor=" << floor << " begin=" << keys.front()
                  << " end=" << chunk_end << " selected=" << keys.size()
                  << " deleted=" << deleted << " elapsed_us=" << elapsed_us
                  << " error=" << static_cast<int64_t>(error);
        if (error != ErrorCode::OK) return error;
        // Below the published floor, batch records are immutable and no new
        // keys can be inserted by a producer. A mismatched count means the
        // backend violated its bounded range/count contract or another owner
        // made progress. Retrying starts with a fresh oldest-key query.
        if (deleted > keys.size()) return ErrorCode::INTERNAL_ERROR;
        if (deleted == 0) return ErrorCode::ETCD_OPERATION_ERROR;
        // Bound the maintenance pass between RPCs, not the server's apply time.
        // The scheduler also yields between passes, allowing snapshot work and
        // lease renewal to run instead of issuing an unbounded deletion loop.
        const auto wait_ms =
            std::min<uint64_t>(config_.batch_interval_ms, 1000);
        const auto wake =
            std::min(deadline, std::chrono::steady_clock::now() +
                                   std::chrono::milliseconds(wait_ms));
        while (std::chrono::steady_clock::now() < wake) {
            if (stopped()) return ErrorCode::OK;
            std::this_thread::sleep_until(
                std::min(wake, std::chrono::steady_clock::now() +
                                   std::chrono::milliseconds(10)));
        }
    }
    // Deliberately do not infer completion from the logical floor or a short
    // final page. Only an empty query in a subsequent pass proves completion.
    return ErrorCode::OK;
}

}  // namespace mooncake
