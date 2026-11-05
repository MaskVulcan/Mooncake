#include "ha_metric_manager.h"
#include "ha/snapshot/batch_oplog/batch_oplog_pruning_coordinator.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "crc32c.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_publisher.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "ha/snapshot/snapshot_maintenance_lease.h"
#ifdef STORE_USE_ETCD
#include <unistd.h>
#include "etcd_helper.h"
#include "ha/kv/etcd_ha_kv_backend.h"
#endif

namespace mooncake::test {
namespace {
class FakeBackend final : public HaKvBackend {
   public:
    ErrorCode DeleteRange(std::string_view begin,
                          std::string_view end) override {
        uint64_t ignored = 0;
        return DeleteRangeCounted(begin, end, ignored);
    }

    ErrorCode DeleteRangeCounted(std::string_view begin, std::string_view end,
                                 uint64_t& deleted) override {
        ++delete_count;
        deleted = 0;
        delete_ranges.emplace_back(begin, end);
        EXPECT_TRUE(values.contains(
            ha::BuildBatchOpLogSnapshotCompactionFloorKey("cluster")));
        if (before_delete) before_delete();
        if (delete_error != ErrorCode::OK && !apply_on_delete_error)
            return delete_error;
        auto first = values.lower_bound(std::string(begin));
        const auto last = values.lower_bound(std::string(end));
        const auto count = static_cast<uint64_t>(std::distance(first, last));
        values.erase(first, last);
        deleted_counts.push_back(count);
        if (delete_error == ErrorCode::OK) deleted = count;
        if (after_delete) after_delete();
        return delete_error;
    }

    ErrorCode Get(std::string_view key, std::string& value) override {
        gets.emplace_back(key);
        auto it = values.find(std::string(key));
        if (it == values.end()) return ErrorCode::ETCD_KEY_NOT_EXIST;
        value = it->second;
        return ErrorCode::OK;
    }

    ErrorCode Put(std::string_view key, std::string_view value) override {
        const std::string owned_key(key);
        values[owned_key] = std::string(value);
        if (!create_revisions.contains(owned_key)) {
            create_revisions[owned_key] = next_create_revision++;
        }
        return ErrorCode::OK;
    }

    ErrorCode Range(std::string_view, std::string_view, size_t,
                    std::vector<KvPair>&) override {
        ADD_FAILURE() << "Pruning must not fetch oplog values";
        return ErrorCode::OK;
    }

    ErrorCode RangeKeys(std::string_view begin, std::string_view end,
                        size_t limit, std::vector<std::string>& keys) override {
        ++range_count;
        keys.clear();
        if (before_range) before_range();
        if (range_error != ErrorCode::OK) return range_error;
        for (auto it = values.lower_bound(std::string(begin));
             it != values.end() && it->first < end && keys.size() < limit; ++it)
            keys.push_back(it->first);
        if (mutate_range_results) mutate_range_results(keys);
        if (after_range) after_range();
        return ErrorCode::OK;
    }

    bool SupportsTxn() const override { return true; }

    ErrorCode Txn(const KvTxn& txn) override {
        ++txn_count;
        if (!mutate_key.empty()) {
            values[mutate_key] = mutate_value;
            mutate_key.clear();
        }
        if (next_txn_error != ErrorCode::OK) {
            const auto error = next_txn_error;
            next_txn_error = ErrorCode::OK;
            return error;
        }
        for (const auto& compare : txn.compares) {
            const auto it = values.find(compare.key);
            if (compare.kind == KvCompareKind::kKeyNotExists) {
                if (it != values.end()) return ErrorCode::ETCD_TRANSACTION_FAIL;
            } else if (compare.kind == KvCompareKind::kCreateRevisionEquals) {
                auto revision = create_revisions.find(compare.key);
                if (revision == create_revisions.end() ||
                    revision->second != compare.expected_revision) {
                    return ErrorCode::ETCD_TRANSACTION_FAIL;
                }
            } else if (it == values.end() ||
                       it->second != compare.expected_value) {
                return ErrorCode::ETCD_TRANSACTION_FAIL;
            }
        }
        for (const auto& put : txn.puts) {
            values[put.key] = put.value;
            if (!create_revisions.contains(put.key)) {
                create_revisions[put.key] = next_create_revision++;
            }
        }
        if (after_txn) after_txn();
        return ErrorCode::OK;
    }

    size_t delete_count{0};
    size_t range_count{0};
    ErrorCode delete_error{ErrorCode::OK};
    ErrorCode range_error{ErrorCode::OK};
    bool apply_on_delete_error{false};
    std::vector<std::pair<std::string, std::string>> delete_ranges;
    std::vector<uint64_t> deleted_counts;
    std::vector<std::string> gets;
    std::function<void()> before_delete;
    std::function<void()> after_delete;
    std::function<void()> before_range;
    std::function<void()> after_range;
    std::function<void(std::vector<std::string>&)> mutate_range_results;
    std::function<void()> after_txn;
    std::map<std::string, std::string> values;
    std::map<std::string, EtcdRevisionId> create_revisions;
    ErrorCode next_txn_error{ErrorCode::OK};
    std::string mutate_key;
    std::string mutate_value;
    size_t txn_count{0};
    EtcdRevisionId next_create_revision{1};
};

class RecordingObjectStore final : public SnapshotObjectStore {
   public:
    tl::expected<void, std::string> UploadBuffer(
        const std::string&, const std::vector<uint8_t>&) override {
        return {};
    }

    tl::expected<void, std::string> DownloadBuffer(
        const std::string& key, std::vector<uint8_t>& bytes) override {
        ++buffer_downloads;
        auto it = objects.find(key);
        if (it == objects.end()) return tl::make_unexpected("missing");
        bytes = it->second;
        return {};
    }

    tl::expected<void, std::string> UploadString(const std::string&,
                                                 const std::string&) override {
        return {};
    }

    tl::expected<void, std::string> DownloadString(
        const std::string& key, std::string& value) override {
        auto it = objects.find(key);
        if (it == objects.end()) return tl::make_unexpected("missing");
        value.assign(it->second.begin(), it->second.end());
        return {};
    }

    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& prefix) override {
        deleted.push_back(prefix);
        return {};
    }

    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string& prefix, std::vector<std::string>& keys) override {
        for (const auto& [key, _] : objects)
            if (key.starts_with(prefix)) keys.push_back(key);
        return {};
    }

    tl::expected<SnapshotObjectInspection, std::string> InspectObject(
        const std::string& key) override {
        auto it = objects.find(key);
        if (it == objects.end()) return tl::make_unexpected("missing");
        if (inspection_error) return tl::make_unexpected("unknown metadata");
        return SnapshotObjectInspection{
            .stored_size = it->second.size(),
            .crc32c = checksums ? std::optional<uint32_t>(
                                      Crc32cValue(it->second.data(),
                                                  it->second.size()) ^
                                      (bad_checksum ? 1 : 0))
                                : std::nullopt};
    }

    std::string GetConnectionInfo() const override { return "test"; }

    bool checksums{true}, bad_checksum{false}, inspection_error{false};
    size_t buffer_downloads{0};
    std::vector<std::string> deleted;
    std::map<std::string, std::vector<uint8_t>> objects;
};

std::string AddSnapshot(RecordingObjectStore& store, uint64_t batch,
                        int64_t lease_id = 101, bool with_weights = false) {
    ha::BatchOpLogSnapshotManifest manifest;
    manifest.snapshot_id = ha::BuildBatchOpLogSnapshotId(batch, lease_id);
    manifest.last_included_batch_id = batch;
    manifest.last_included_seq = batch * 10;
    manifest.producer_view_version = 7;
    const std::string root = "snapshots";
    manifest.segments = {
        ha::BuildBatchOpLogSnapshotSegmentsKey(root, manifest.snapshot_id), 1,
        Crc32cValue("s", 1)};
    manifest.object_chunks.push_back({0,
                                      ha::BuildBatchOpLogSnapshotObjectChunkKey(
                                          root, manifest.snapshot_id, 0),
                                      1, 1, Crc32cValue("o", 1)});
    store.objects[manifest.segments.key] = {'s'};
    store.objects[manifest.object_chunks[0].key] = {'o'};
    if (with_weights) {
        manifest.schema_version = ha::kBatchOpLogWeightSnapshotSchemaVersion;
        manifest.snapshot_format = ha::kBatchOpLogWeightSnapshotFormat;
        manifest.weight_metadata = ha::BatchOpLogSnapshotObjectDescriptor{
            ha::BuildBatchOpLogSnapshotWeightMetadataKey(root,
                                                         manifest.snapshot_id),
            1, Crc32cValue("w", 1)};
        store.objects[manifest.weight_metadata->key] = {'w'};
    }
    const auto bytes = ha::EncodeBatchOpLogSnapshotManifest(manifest);
    ha::BatchOpLogSnapshotDescriptor descriptor;
    descriptor.schema_version = manifest.schema_version;
    descriptor.snapshot_format = manifest.snapshot_format;
    descriptor.snapshot_id = manifest.snapshot_id;
    descriptor.last_included_batch_id = batch;
    descriptor.last_included_seq = manifest.last_included_seq;
    descriptor.producer_view_version = manifest.producer_view_version;
    descriptor.manifest_key =
        ha::BuildBatchOpLogSnapshotManifestKey(root, manifest.snapshot_id);
    descriptor.manifest_size = bytes.size();
    descriptor.manifest_crc32c = Crc32cValue(bytes.data(), bytes.size());
    descriptor.created_at_ms = 1;
    store.objects[descriptor.manifest_key] = {bytes.begin(), bytes.end()};
    const auto json = ha::EncodeBatchOpLogSnapshotDescriptor(descriptor);
    store.objects[ha::BuildBatchOpLogSnapshotDescriptorKey(
        root, manifest.snapshot_id)] = {json.begin(), json.end()};
    return json;
}

class PruningTest : public ::testing::Test {
   protected:
    void SetUp() override {
        HAMetricManager::instance().reset_snapshot_runtime(true);
        ASSERT_EQ(ErrorCode::OK, backend.Put(lock_key, "101"));
        latest = AddSnapshot(store, 20);
        fallback = AddSnapshot(store, 10);
        backend.Put(latest_key, latest);
        backend.Put(fallback_key, fallback);
        backend.Put(BuildBatchRecordKey("cluster", 10), "batch");
        backend.Put(BuildBatchRecordKey("cluster", 20), "batch");
    }
    ErrorCode Run() { return pruning.Run(*lease); }
    void SetBatches(std::initializer_list<uint64_t> ids) {
        const auto first = BuildBatchRecordKey("cluster", 0);
        const auto last = BuildBatchRecordRange("cluster", UINT64_MAX).end_key;
        backend.values.erase(backend.values.lower_bound(first),
                             backend.values.lower_bound(last));
        for (const auto id : ids)
            backend.Put(BuildBatchRecordKey("cluster", id), "batch");
    }
    const std::string latest_key =
        ha::BuildBatchOpLogSnapshotLatestKey("cluster");
    const std::string fallback_key =
        ha::BuildBatchOpLogSnapshotFallbackKey("cluster");
    const std::string lock_key =
        ha::BuildBatchOpLogSnapshotMaintenanceKey("cluster");
    const std::string floor_key =
        ha::BuildBatchOpLogSnapshotCompactionFloorKey("cluster");
    FakeBackend backend;
    RecordingObjectStore store;
    std::unique_ptr<SnapshotMaintenanceLease> lease =
        SnapshotMaintenanceLease::MakeForTesting("cluster", "101");
    BatchOpLogPruningCoordinator pruning{backend, store, "cluster",
                                         "snapshots"};
    std::string latest, fallback;
};

TEST_F(PruningTest, MissingWeightArtifactDoesNotAdvanceFloorOrDeleteLog) {
    latest = AddSnapshot(store, 20, 101, true);
    fallback = AddSnapshot(store, 10, 101, true);
    backend.Put(latest_key, latest);
    backend.Put(fallback_key, fallback);
    for (const auto& id : {"20-101", "10-101"}) {
        const auto key =
            ha::BuildBatchOpLogSnapshotWeightMetadataKey("snapshots", id);
        const auto bytes = store.objects.at(key);
        store.objects.erase(key);
        EXPECT_NE(ErrorCode::OK, Run());
        EXPECT_FALSE(backend.values.contains(floor_key));
        EXPECT_EQ(0u, backend.delete_count);
        store.objects[key] = bytes;
    }
    EXPECT_EQ(ErrorCode::OK, Run());
    EXPECT_EQ("10", backend.values.at(floor_key));
    EXPECT_EQ(1u, backend.delete_count);
}

TEST_F(PruningTest, FirstSnapshotDoesNotPublishFloor) {
    backend.values.erase(fallback_key);
    EXPECT_EQ(ErrorCode::OK, Run());
    EXPECT_FALSE(backend.values.contains(floor_key));
    EXPECT_EQ(0u, backend.txn_count);
    EXPECT_EQ(HAMetricManager::SnapshotSkipReason::NoFallback,
              HAMetricManager::instance().get_snapshot_runtime().skip_reason);
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, PublishesFloorBeforeDeletionAndRotates) {
    backend.before_delete = [&] { EXPECT_EQ("10", backend.values[floor_key]); };
    ASSERT_EQ(ErrorCode::OK, Run());
    EXPECT_EQ(1u, backend.delete_count);
    backend.before_delete = [&] { EXPECT_EQ("20", backend.values[floor_key]); };
    ASSERT_EQ(ErrorCode::OK, BatchOpLogSnapshotPublisher(backend, "cluster")
                                 .Publish(*lease, AddSnapshot(store, 30)));
    ASSERT_EQ(ErrorCode::OK, Run());
    EXPECT_EQ(2u, backend.delete_count);
}

TEST_F(PruningTest, RequiresIndependentOrderedSnapshots) {
    for (const auto& value :
         {latest, AddSnapshot(store, 20, 102), AddSnapshot(store, 30)}) {
        backend.values[fallback_key] = value;
        EXPECT_EQ(ErrorCode::OK, Run());
        EXPECT_FALSE(backend.values.contains(floor_key));
    }
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, RejectsCorruptPointersAndArtifacts) {
    const auto original_objects = store.objects;
    for (const auto& pointer_key : {latest_key, fallback_key}) {
        const auto pointer = backend.values[pointer_key];
        backend.values[pointer_key] = "invalid";
        EXPECT_NE(ErrorCode::OK, Run());
        backend.values[pointer_key] = pointer;
    }
    // Exercise every artifact of both snapshots, including descriptor raw
    // bytes, manifest CRC, segments and chunks.
    for (const auto& [key, bytes] : original_objects) {
        SCOPED_TRACE(key);
        store.objects.erase(key);
        EXPECT_NE(ErrorCode::OK, Run());
        store.objects = original_objects;
        store.objects[key].push_back('x');
        EXPECT_NE(ErrorCode::OK, Run());
        store.objects = original_objects;
        store.objects[key][0] ^= 1;
        EXPECT_NE(ErrorCode::OK, Run());
        store.objects = original_objects;
    }
    EXPECT_FALSE(backend.values.contains(floor_key));
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, RejectsManifestIdentityDespiteValidCrc) {
    const auto original_objects = store.objects;
    for (const auto& key : {latest_key, fallback_key}) {
        store.objects = original_objects;
        const auto original = backend.values[key];
        auto descriptor = *ha::DecodeBatchOpLogSnapshotDescriptor(original);
        const auto manifest_bytes = store.objects[descriptor.manifest_key];
        auto manifest = *ha::DecodeBatchOpLogSnapshotManifest(
            std::string(manifest_bytes.begin(), manifest_bytes.end()));
        ++manifest.last_included_seq;
        const auto bytes = ha::EncodeBatchOpLogSnapshotManifest(manifest);
        store.objects[descriptor.manifest_key] = {bytes.begin(), bytes.end()};
        descriptor.manifest_size = bytes.size();
        descriptor.manifest_crc32c = Crc32cValue(bytes.data(), bytes.size());
        const auto json = ha::EncodeBatchOpLogSnapshotDescriptor(descriptor);
        backend.values[key] = json;
        store.objects[ha::BuildBatchOpLogSnapshotDescriptorKey(
            "snapshots", descriptor.snapshot_id)] = {json.begin(), json.end()};
        EXPECT_NE(ErrorCode::OK, Run());
        backend.values[key] = original;
    }
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, InspectionUnknownOrChecksumMismatchFailsClosed) {
    store.bad_checksum = true;
    EXPECT_NE(ErrorCode::OK, Run());
    store.bad_checksum = false;
    store.inspection_error = true;
    EXPECT_NE(ErrorCode::OK, Run());
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, UnsupportedChecksumNeverDownloadsChunkBodies) {
    store.checksums = false;
    EXPECT_EQ(ErrorCode::OK, Run());
    EXPECT_EQ(0u, store.buffer_downloads);
    EXPECT_EQ(1u, backend.delete_count);
}

TEST_F(PruningTest, CasFencesPointersLeaseAndRawFloor) {
    for (const auto& key : {latest_key, fallback_key, lock_key, floor_key}) {
        SCOPED_TRACE(key);
        const auto values = backend.values;
        backend.mutate_key = key;
        backend.mutate_value = "changed";
        EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, Run());
        EXPECT_EQ(0u, backend.delete_count);
        backend.values = values;
    }
    backend.values[floor_key] = "0009";
    backend.mutate_key = floor_key;
    backend.mutate_value = "9";
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, Run());
    backend.create_revisions[lock_key] = 99;
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, Run());
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, FailedOrUnknownTxnDoesNotDelete) {
    for (auto error :
         {ErrorCode::ETCD_OPERATION_ERROR, ErrorCode::ETCD_TRANSACTION_FAIL}) {
        backend.next_txn_error = error;
        EXPECT_EQ(error, Run());
        EXPECT_EQ(0u, backend.delete_count);
    }
}

TEST_F(PruningTest, DeleteFailureKeepsFloorAndRetries) {
    auto& metrics = HAMetricManager::instance();
    const auto before = metrics.get_snapshot_runtime().floor_advances_total;
    const auto errors =
        metrics
            .get_snapshot_operation(HAMetricManager::SnapshotOperation::Prune)
            .errors;
    backend.delete_error = ErrorCode::ETCD_OPERATION_ERROR;
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, Run());
    EXPECT_EQ("10", backend.values[floor_key]);
    EXPECT_EQ(10u, metrics.get_snapshot_runtime().compaction_floor);
    EXPECT_EQ(before + 1, metrics.get_snapshot_runtime().floor_advances_total);
    EXPECT_EQ(errors + 1, metrics
                              .get_snapshot_operation(
                                  HAMetricManager::SnapshotOperation::Prune)
                              .errors);
    backend.delete_error = ErrorCode::OK;
    EXPECT_EQ(ErrorCode::OK, Run());
    EXPECT_EQ(before + 1, metrics.get_snapshot_runtime().floor_advances_total);
    EXPECT_EQ(errors + 1, metrics
                              .get_snapshot_operation(
                                  HAMetricManager::SnapshotOperation::Prune)
                              .errors);
    EXPECT_EQ(2u, backend.delete_count);
    EXPECT_EQ("10", backend.values[floor_key]);
}

TEST_F(PruningTest, CrashAfterCasLeavesReaderVisibleFloorForRetry) {
    bool stopped = false;
    bool more = false;
    backend.after_txn = [&] { stopped = true; };
    EXPECT_EQ(ErrorCode::OK,
              pruning.Run(*lease, [&] { return stopped; }, &more));
    EXPECT_TRUE(more);
    EXPECT_EQ("10", backend.values[floor_key]);
    EXPECT_EQ(0u, backend.delete_count);
    backend.after_txn = {};
    EXPECT_EQ(ErrorCode::OK, pruning.Resume(*lease, {}, &more));
    EXPECT_FALSE(more);
    EXPECT_EQ(1u, backend.delete_count);
    EXPECT_EQ(1u, backend.txn_count);
}

TEST_F(PruningTest, FloorNeverRegressesAndParsingIsStrict) {
    backend.values[floor_key] = "11";
    EXPECT_EQ(ErrorCode::OK, Run());
    EXPECT_EQ("11", backend.values[floor_key]);
    EXPECT_EQ(0u, backend.delete_count);
    for (const auto& value :
         {"", "-1", "+1", " 1", "1x", "18446744073709551616"}) {
        backend.values[floor_key] = value;
        EXPECT_NE(ErrorCode::OK, Run());
    }
    EXPECT_EQ(0u, backend.txn_count);
    backend.values[floor_key] = "0009";
    EXPECT_EQ(ErrorCode::OK, Run());
    EXPECT_EQ("10", backend.values[floor_key]);
}

TEST_F(PruningTest, CancelledOrReleasedLeaseDoesNotDelete) {
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              pruning.Run(*lease, [] { return true; }));
    ASSERT_EQ(ErrorCode::OK, lease->Release());
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, Run());
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, ChunksAtTheLimitAndOneAboveItWithoutFetchingValues) {
    for (const size_t count : {3u, 4u, 5u}) {
        SCOPED_TRACE(count);
        SetBatches({});
        for (size_t id = 0; id < count; ++id)
            backend.Put(BuildBatchRecordKey("cluster", id), "batch");
        backend.deleted_counts.clear();
        BatchOpLogPruningCoordinator bounded(
            backend, store, "cluster", "snapshots",
            {.max_batches_per_request = 4, .batch_interval_ms = 0});
        bool more = true;
        ASSERT_EQ(ErrorCode::OK, bounded.Run(*lease, {}, &more));
        EXPECT_FALSE(more);
        ASSERT_EQ(count <= 4 ? 1u : 2u, backend.deleted_counts.size());
        EXPECT_EQ(std::min<size_t>(count, 4), backend.deleted_counts[0]);
        if (count == 5) EXPECT_EQ(1u, backend.deleted_counts[1]);
        EXPECT_EQ("10", backend.values[floor_key]);
    }
}

TEST_F(PruningTest, ResumeUsesPublishedFloorWithoutSnapshotPointers) {
    backend.values[floor_key] = "10";
    backend.values.erase(latest_key);
    backend.values.erase(fallback_key);
    store.objects.clear();
    bool more = true;
    EXPECT_EQ(ErrorCode::OK, pruning.Resume(*lease, {}, &more));
    EXPECT_FALSE(more);
    EXPECT_EQ(0u, backend.txn_count);
    ASSERT_EQ(1u, backend.gets.size());
    EXPECT_EQ(floor_key, backend.gets[0]);
    EXPECT_FALSE(backend.values.contains(BuildBatchRecordKey("cluster", 10)));
    EXPECT_TRUE(backend.values.contains(BuildBatchRecordKey("cluster", 20)));
}

TEST_F(PruningTest, ResumeAbsentFloorAndStrictFloorParsing) {
    bool more = true;
    EXPECT_EQ(ErrorCode::OK, pruning.Resume(*lease, {}, &more));
    EXPECT_FALSE(more);
    EXPECT_EQ(0u, backend.range_count);
    for (const auto& value :
         {"", "-1", "+1", " 1", "1x", "18446744073709551616"}) {
        backend.values[floor_key] = value;
        EXPECT_EQ(ErrorCode::INTERNAL_ERROR, pruning.Resume(*lease));
    }
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, ResumeDeletesZeroAndMaxIdWhileProtectingOtherNamespaces) {
    SetBatches({0, UINT64_MAX - 1, UINT64_MAX});
    backend.values[floor_key] = std::to_string(UINT64_MAX);
    const auto other = BuildBatchRecordKey("other", 0);
    const auto control = BuildDurablePrefixKey("cluster");
    backend.Put(other, "other");
    backend.Put(control, "durable");
    BatchOpLogPruningCoordinator bounded(
        backend, store, "cluster", "snapshots",
        {.max_batches_per_request = 2, .batch_interval_ms = 0});
    bool more = true;
    EXPECT_EQ(ErrorCode::OK, bounded.Resume(*lease, {}, &more));
    EXPECT_FALSE(more);
    ASSERT_EQ(2u, backend.deleted_counts.size());
    EXPECT_EQ(2u, backend.deleted_counts[0]);
    EXPECT_EQ(1u, backend.deleted_counts[1]);
    EXPECT_TRUE(backend.values.contains(other));
    EXPECT_TRUE(backend.values.contains(control));
    EXPECT_TRUE(backend.values.contains(latest_key));
    EXPECT_TRUE(backend.values.contains(lock_key));
}

TEST_F(PruningTest, QueriesActualOldestKeysAcrossLargeHoles) {
    SetBatches({2, 1000000, 9000000000000ULL});
    backend.values[floor_key] = "9000000000000";
    BatchOpLogPruningCoordinator bounded(
        backend, store, "cluster", "snapshots",
        {.max_batches_per_request = 2, .batch_interval_ms = 0});
    bool more = true;
    ASSERT_EQ(ErrorCode::OK, bounded.Resume(*lease, {}, &more));
    EXPECT_FALSE(more);
    ASSERT_EQ(2u, backend.delete_ranges.size());
    EXPECT_EQ(BuildBatchRecordKey("cluster", 2),
              backend.delete_ranges[0].first);
    EXPECT_EQ(BuildBatchRecordKey("cluster", 1000001),
              backend.delete_ranges[0].second);
    EXPECT_EQ(BuildBatchRecordKey("cluster", 9000000000000ULL),
              backend.delete_ranges[1].first);
    EXPECT_EQ(3u, backend.range_count);
}

TEST_F(PruningTest, PartialFailureResumesOldestRemainingWithoutFloorAdvance) {
    SetBatches({1, 2, 3, 4, 5});
    BatchOpLogPruningCoordinator bounded(
        backend, store, "cluster", "snapshots",
        {.max_batches_per_request = 2, .batch_interval_ms = 0});
    backend.before_delete = [&] {
        if (backend.delete_count == 2)
            backend.delete_error = ErrorCode::ETCD_OPERATION_ERROR;
    };
    bool more = false;
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, bounded.Run(*lease, {}, &more));
    EXPECT_TRUE(more);
    EXPECT_EQ("10", backend.values[floor_key]);
    EXPECT_FALSE(backend.values.contains(BuildBatchRecordKey("cluster", 1)));
    EXPECT_TRUE(backend.values.contains(BuildBatchRecordKey("cluster", 3)));
    backend.before_delete = {};
    backend.delete_error = ErrorCode::OK;
    EXPECT_EQ(ErrorCode::OK, bounded.Resume(*lease, {}, &more));
    EXPECT_FALSE(more);
    EXPECT_EQ(1u, backend.txn_count);
    ASSERT_EQ(4u, backend.delete_ranges.size());
    EXPECT_EQ(backend.delete_ranges[1], backend.delete_ranges[2]);
}

TEST_F(PruningTest, UnknownDeleteOutcomeRequeriesInsteadOfReplayingOldRange) {
    SetBatches({1, 2, 3});
    backend.values[floor_key] = "10";
    BatchOpLogPruningCoordinator bounded(
        backend, store, "cluster", "snapshots",
        {.max_batches_per_request = 2, .batch_interval_ms = 0});
    backend.delete_error = ErrorCode::ETCD_OPERATION_ERROR;
    backend.apply_on_delete_error = true;
    bool more = false;
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR,
              bounded.Resume(*lease, {}, &more));
    EXPECT_TRUE(more);
    backend.delete_error = ErrorCode::OK;
    EXPECT_EQ(ErrorCode::OK, bounded.Resume(*lease, {}, &more));
    EXPECT_FALSE(more);
    ASSERT_EQ(2u, backend.delete_ranges.size());
    EXPECT_EQ(BuildBatchRecordKey("cluster", 3),
              backend.delete_ranges[1].first);
}

TEST_F(PruningTest, RejectsInvalidKeyPagesBeforeDeletingAnything) {
    backend.values[floor_key] = "10";
    const std::vector<std::vector<std::string>> pages = {
        {BuildBatchRecordKey("other", 1)},
        {BuildBatchRecordKey("cluster", 11)},
        {BuildBatchRecordKey("cluster", 2), BuildBatchRecordKey("cluster", 1)},
        {BuildBatchRecordKey("cluster", 1), BuildBatchRecordKey("cluster", 1)},
        {"/oplog/cluster/batches/not-a-batch"},
        {BuildBatchRecordKey("cluster", 1) + "extra"},
        {BuildBatchRecordKey("cluster", 1), BuildBatchRecordKey("cluster", 2),
         BuildBatchRecordKey("cluster", 3)}};
    BatchOpLogPruningCoordinator bounded(
        backend, store, "cluster", "snapshots",
        {.max_batches_per_request = 2, .batch_interval_ms = 0});
    for (const auto& page : pages) {
        backend.mutate_range_results = [&](auto& keys) { keys = page; };
        bool more = false;
        EXPECT_EQ(ErrorCode::INTERNAL_ERROR, bounded.Resume(*lease, {}, &more));
        EXPECT_TRUE(more);
        EXPECT_EQ(0u, backend.delete_count);
    }
}

TEST_F(PruningTest, FailedKeysOnlyQueryRetainsWorkAndDoesNotDelete) {
    backend.values[floor_key] = "10";
    backend.range_error = ErrorCode::ETCD_OPERATION_ERROR;
    bool more = false;
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR,
              pruning.Resume(*lease, {}, &more));
    EXPECT_TRUE(more);
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, CancellationAfterRangeDoesNotIssueDelete) {
    backend.values[floor_key] = "10";
    bool cancelled = false;
    backend.after_range = [&] { cancelled = true; };
    bool more = false;
    EXPECT_EQ(ErrorCode::OK,
              pruning.Resume(*lease, [&] { return cancelled; }, &more));
    EXPECT_TRUE(more);
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, LeaseLossBetweenChunksStopsImmediately) {
    SetBatches({1, 2, 3});
    backend.values[floor_key] = "10";
    backend.after_delete = [&] { EXPECT_EQ(ErrorCode::OK, lease->Release()); };
    BatchOpLogPruningCoordinator bounded(
        backend, store, "cluster", "snapshots",
        {.max_batches_per_request = 1, .batch_interval_ms = 1000});
    const auto start = std::chrono::steady_clock::now();
    bool more = false;
    EXPECT_EQ(ErrorCode::OK, bounded.Resume(*lease, {}, &more));
    EXPECT_LT(std::chrono::steady_clock::now() - start,
              std::chrono::milliseconds(500));
    EXPECT_TRUE(more);
    EXPECT_EQ(1u, backend.delete_count);
    EXPECT_EQ(1u, backend.range_count);
}

TEST_F(PruningTest, CancellationIsObservedDuringTheBatchInterval) {
    SetBatches({1, 2});
    backend.values[floor_key] = "10";
    bool applied = false;
    size_t interval_checks = 0;
    backend.after_delete = [&] { applied = true; };
    BatchOpLogPruningCoordinator bounded(
        backend, store, "cluster", "snapshots",
        {.max_batches_per_request = 1, .batch_interval_ms = 1000});
    bool more = false;
    EXPECT_EQ(
        ErrorCode::OK,
        bounded.Resume(
            *lease, [&] { return applied && ++interval_checks == 3; }, &more));
    EXPECT_TRUE(more);
    EXPECT_EQ(3u, interval_checks);
    EXPECT_EQ(1u, backend.delete_count);
    EXPECT_EQ(1u, backend.range_count);
}

TEST_F(PruningTest, EachPassIsBoundedAndContinuationDoesNotNeedNewSnapshot) {
    SetBatches({});
    for (uint64_t id = 0; id < 33; ++id)
        backend.Put(BuildBatchRecordKey("cluster", id), "batch");
    backend.values[floor_key] = "32";
    BatchOpLogPruningCoordinator bounded(
        backend, store, "cluster", "snapshots",
        {.max_batches_per_request = 2, .batch_interval_ms = 0});
    bool more = false;
    EXPECT_EQ(ErrorCode::OK, bounded.Resume(*lease, {}, &more));
    EXPECT_TRUE(more);
    EXPECT_EQ(16u, backend.delete_count);
    EXPECT_EQ(16u, backend.range_count);
    EXPECT_EQ(ErrorCode::OK, bounded.Resume(*lease, {}, &more));
    EXPECT_FALSE(more);
    EXPECT_EQ(17u, backend.delete_count);
    EXPECT_EQ(0u, backend.txn_count);
}

TEST_F(PruningTest, SlowEnumerationStillAllowsOneChunkToMakeProgress) {
    SetBatches({1, 2});
    backend.values[floor_key] = "10";
    backend.before_range = [] {
        std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    };
    BatchOpLogPruningCoordinator bounded(
        backend, store, "cluster", "snapshots",
        {.max_batches_per_request = 1, .batch_interval_ms = 0});
    bool more = false;
    EXPECT_EQ(ErrorCode::OK, bounded.Resume(*lease, {}, &more));
    EXPECT_TRUE(more);
    EXPECT_EQ(1u, backend.range_count);
    EXPECT_EQ(1u, backend.delete_count);
    EXPECT_FALSE(backend.values.contains(BuildBatchRecordKey("cluster", 1)));
    EXPECT_TRUE(backend.values.contains(BuildBatchRecordKey("cluster", 2)));
}

TEST_F(PruningTest, InvalidConfigurationAndWrongNamespaceFailClosed) {
    for (const size_t limit :
         {size_t{0},
          static_cast<size_t>(std::numeric_limits<int>::max()) + 1}) {
        BatchOpLogPruningCoordinator invalid(
            backend, store, "cluster", "snapshots",
            {.max_batches_per_request = limit, .batch_interval_ms = 0});
        EXPECT_EQ(ErrorCode::INVALID_PARAMS, invalid.Run(*lease));
        EXPECT_EQ(ErrorCode::INVALID_PARAMS, invalid.Resume(*lease));
    }
    BatchOpLogPruningCoordinator wrong_namespace(backend, store, "other",
                                                 "snapshots");
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, wrong_namespace.Resume(*lease));
    BatchOpLogPruningCoordinator huge_interval(
        backend, store, "cluster", "snapshots",
        {.max_batches_per_request = 1,
         .batch_interval_ms =
             uint64_t{std::numeric_limits<uint32_t>::max()} + 1});
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, huge_interval.Run(*lease));
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, huge_interval.Resume(*lease));
    EXPECT_EQ(0u, backend.txn_count);
    EXPECT_EQ(0u, backend.delete_count);
}

#ifdef STORE_USE_ETCD
TEST(PruningRealEtcdTest, FloorPrecedesInclusiveDeleteAndProtectsControlKeys) {
    const char* endpoints = std::getenv("MOONCAKE_TEST_ETCD_ENDPOINTS");
    if (endpoints == nullptr && std::getenv("CI") != nullptr) {
        endpoints = "http://127.0.0.1:2379";
    }
    if (endpoints == nullptr)
        GTEST_SKIP() << "Set MOONCAKE_TEST_ETCD_ENDPOINTS for isolated etcd";
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::ConnectToEtcdStoreClient(endpoints));
    const std::string cluster = "bounded-pruning-" + std::to_string(getpid());
    EtcdHaKvBackend backend;
    const std::string root = "/oplog/" + cluster + "/";
    std::string ignored;
    const auto probe = backend.Get(root + "probe", ignored);
    ASSERT_TRUE(probe == ErrorCode::OK ||
                probe == ErrorCode::ETCD_KEY_NOT_EXIST)
        << "etcd is not reachable at " << endpoints;
    ASSERT_EQ(ErrorCode::OK, backend.DeleteRange(root, root + "\xff"));
    SnapshotMaintenanceLease lease(cluster);
    ASSERT_EQ(ErrorCode::OK, lease.Acquire());
    RecordingObjectStore store;
    BatchOpLogSnapshotPublisher publisher(backend, cluster);
    ASSERT_EQ(
        ErrorCode::OK,
        publisher.Publish(lease, AddSnapshot(store, 10, lease.lease_id())));
    ASSERT_EQ(
        ErrorCode::OK,
        publisher.Publish(lease, AddSnapshot(store, 20, lease.lease_id())));
    for (uint64_t id : {0, 1, 10, 11, 20, 21})
        ASSERT_EQ(ErrorCode::OK,
                  backend.Put(BuildBatchRecordKey(cluster, id), "batch"));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey(cluster), "durable"));
    ASSERT_EQ(ErrorCode::OK, backend.Put(BuildProducerViewKey(cluster), "7"));
    BatchOpLogPruningCoordinator pruning(
        backend, store, cluster, "snapshots",
        {.max_batches_per_request = 2, .batch_interval_ms = 0});
    ASSERT_EQ(ErrorCode::OK, pruning.Run(lease));
    ASSERT_EQ(
        ErrorCode::OK,
        backend.Get(ha::BuildBatchOpLogSnapshotCompactionFloorKey(cluster),
                    ignored));
    EXPECT_EQ("10", ignored);
    for (uint64_t id : {0, 1, 10})
        EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST,
                  backend.Get(BuildBatchRecordKey(cluster, id), ignored));
    for (const auto& key :
         {BuildBatchRecordKey(cluster, 11), BuildBatchRecordKey(cluster, 20),
          BuildBatchRecordKey(cluster, 21), BuildDurablePrefixKey(cluster),
          BuildProducerViewKey(cluster),
          ha::BuildBatchOpLogSnapshotLatestKey(cluster),
          ha::BuildBatchOpLogSnapshotFallbackKey(cluster), lease.lock_key()})
        EXPECT_EQ(ErrorCode::OK, backend.Get(key, ignored));
    EXPECT_EQ(ErrorCode::OK, pruning.Run(lease));
    ASSERT_EQ(ErrorCode::OK, lease.Release());
    ASSERT_EQ(ErrorCode::OK, backend.DeleteRange(root, root + "\xff"));
}
#endif

}  // namespace
}  // namespace mooncake::test
