#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <memory>
#include <unordered_set>
#include <future>
#include <string>
#include <thread>

#include "allocator.h"
#include "client_metric.h"
#include "common/network.h"
#include "file_storage.h"
#include "ssd_control_rpc.h"
#include "ssd_file_storage_owner_reader.h"
#include "ssd_transfer_registration.h"
#include "ssd_transfer_target.h"
#include "transfer_engine.h"
#include "storage_backend.h"
#include "tenant_id.h"
#include "test_server_helpers.h"
#include "common/client_buffer_allocation.h"
#include "utils/common.h"

namespace mooncake {

void SetEnv(const std::string& key, const std::string& value) {
    setenv(key.c_str(), value.c_str(), 1);
}

void UnsetEnv(const std::string& key) { unsetenv(key.c_str()); }

// Commit another offload after grouping but before this batch reaches the
// backend. This deterministically exercises the duplicate race from #4097
// without relying on a second thread winning a filesystem/RPC race.
class ConcurrentOffloadBackend : public BucketStorageBackend {
   public:
    ConcurrentOffloadBackend(const FileStorageConfig& config,
                             const BucketBackendConfig& bucket_config,
                             std::unordered_set<std::string> duplicate_keys,
                             bool fail_write)
        : BucketStorageBackend(config, bucket_config),
          duplicate_keys_(std::move(duplicate_keys)),
          fail_write_(fail_write) {}

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch,
        std::function<ErrorCode(const std::vector<std::string>&,
                                std::vector<StorageObjectMetadata>&)>
            complete,
        EvictionHandler eviction = nullptr) override {
        std::unordered_map<std::string, std::vector<Slice>> duplicates;
        for (const auto& key : duplicate_keys_) {
            duplicates.emplace(key, batch.at(key));
        }
        if (!duplicates.empty()) {
            // The local commit survives even if its Master notification was
            // lost. The retry must release its own task without claiming a
            // new disk replica for the skipped write.
            auto result =
                BucketStorageBackend::BatchOffload(duplicates, nullptr);
            if (!result) return result;
        }
        if (fail_write_) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        return BucketStorageBackend::BatchOffload(batch, std::move(complete),
                                                  std::move(eviction));
    }

   private:
    std::unordered_set<std::string> duplicate_keys_;
    bool fail_write_;
};

class FileStorageTest : public ::testing::Test {
   protected:
    std::string data_path;

    void RunSkippedOffloadScenario(
        size_t duplicate_count, bool fail_write,
        StorageBackendType backend_type = StorageBackendType::kBucket) {
        testing::InProcMaster master;
        ASSERT_TRUE(master.Start(InProcMasterConfigBuilder()
                                     .set_enable_offload(true)
                                     .set_default_kv_lease_ttl(0)
                                     .set_root_fs_dir("")
                                     .build()));
        constexpr size_t kSegmentSize = 16 * 1024 * 1024;
        std::unique_ptr<void, decltype(&std::free)> segment(
            allocate_buffer_allocator_memory(kSegmentSize), &std::free);
        ASSERT_NE(segment, nullptr);
        SimpleAllocator allocator(kSegmentSize);
        const auto endpoint = "127.0.0.1:" + std::to_string(getFreeTcpPort());
        auto client_result =
            Client::Create(endpoint, master.metadata_url(), "tcp", std::nullopt,
                           master.master_address());
        ASSERT_TRUE(client_result.has_value());
        auto client = client_result.value();
        ASSERT_TRUE(client->MountSegment(segment.get(), kSegmentSize, "tcp"));
        ASSERT_TRUE(client->RegisterLocalMemory(
            allocator.getBase(), kSegmentSize, "cpu:0", false, false));
        ASSERT_TRUE(client->MountLocalDiskSegment(true));

        const std::vector<std::string> keys = {"duplicate_key", "other_key"};
        std::vector<std::string> values = {std::string(128, 'a'),
                                           std::string(128, 'b')};
        ReplicateConfig replicate;
        replicate.replica_num = 1;
        for (size_t i = 0; i < keys.size(); ++i) {
            void* buffer = allocator.allocate(values[i].size());
            ASSERT_NE(buffer, nullptr);
            std::memcpy(buffer, values[i].data(), values[i].size());
            std::vector<Slice> slices{{buffer, values[i].size()}};
            ASSERT_TRUE(client->Put(keys[i], slices, replicate));
            allocator.deallocate(buffer, values[i].size());
        }
        std::vector<OffloadTaskItem> tasks;
        ASSERT_TRUE(client->OffloadObjectHeartbeat(true, tasks));
        ASSERT_EQ(tasks.size(), keys.size());
        void* overwrite_buffer = allocator.allocate(values.front().size());
        ASSERT_NE(overwrite_buffer, nullptr);
        std::memset(overwrite_buffer, 'c', values.front().size());
        std::vector<Slice> overwrite{{overwrite_buffer, values.front().size()}};
        for (const auto& key : keys) {
            auto blocked = client->Upsert(key, overwrite, replicate);
            ASSERT_FALSE(blocked.has_value());
            ASSERT_EQ(blocked.error(), ErrorCode::OBJECT_HAS_REPLICATION_TASK);
        }

        FileStorageConfig config = FileStorageConfig::FromEnvironment();
        config.storage_backend_type = backend_type;
        config.storage_filepath = data_path + "/skipped_ssd";
        config.local_buffer_size = 1024 * 1024;
        fs::create_directories(config.storage_filepath);
        FileStorage file_storage(config, client, endpoint);
        BucketBackendConfig bucket_config;
        bucket_config.bucket_keys_limit = keys.size();
        std::unordered_set<std::string> duplicates;
        for (size_t i = 0; i < duplicate_count; ++i) {
            duplicates.insert(TenantId::Default().MakeScopedKey(keys[i]));
        }
        if (backend_type == StorageBackendType::kBucket) {
            file_storage.storage_backend_ =
                std::make_shared<ConcurrentOffloadBackend>(
                    config, bucket_config, std::move(duplicates), fail_write);
        } else {
            // File-per-key also returns success for a partially written batch
            // and omits failed keys from its completion callback.
            file_storage.storage_backend_->SetTestFailurePredicate(
                [duplicates](const std::string& key) {
                    return duplicates.count(key) != 0;
                });
        }
        auto backend = file_storage.storage_backend_;
        ASSERT_TRUE(backend->Init());
        auto result = file_storage.OffloadObjects(tasks);
        if (fail_write) {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error(), ErrorCode::FILE_WRITE_FAIL);
        } else {
            ASSERT_TRUE(result.has_value());
        }

        for (size_t i = 0; i < keys.size(); ++i) {
            const auto storage_key = TenantId::Default().MakeScopedKey(keys[i]);
            const bool persisted = backend_type == StorageBackendType::kBucket
                                       ? !fail_write || i < duplicate_count
                                       : i >= duplicate_count;
            auto exists = backend->IsExist(storage_key);
            ASSERT_TRUE(exists.has_value());
            EXPECT_EQ(exists.value(), persisted);
            if (persisted) {
                std::string actual(values[i].size(), '\0');
                std::unordered_map<std::string, Slice> read{
                    {storage_key, {actual.data(), actual.size()}}};
                ASSERT_TRUE(backend->BatchLoad(read));
                EXPECT_EQ(actual, values[i]);
            }
            auto query = client->Query(keys[i]);
            ASSERT_TRUE(query.has_value());
            size_t disk_replicas = 0;
            for (const auto& replica : query->replicas) {
                disk_replicas += replica.is_local_disk_replica();
            }
            // Only newly committed keys may register a disk replica. A skip
            // is a NACK, never a success with a fabricated bucket descriptor.
            EXPECT_EQ(disk_replicas,
                      (!fail_write && i >= duplicate_count) ? 1u : 0u);
            // Upsert is blocked both by an in-flight offload marker and by a
            // retained source reference. It must become available immediately,
            // without waiting for the offload task timeout.
            auto overwritten = client->Upsert(keys[i], overwrite, replicate);
            EXPECT_TRUE(overwritten.has_value())
                << keys[i] << ": " << toString(overwritten.error());
        }
        allocator.deallocate(overwrite_buffer, values.front().size());
        EXPECT_TRUE(client->UnmountSegment(segment.get(), kSegmentSize));
        EXPECT_TRUE(client->unregisterLocalMemory(allocator.getBase()));
    }

    void RunOwnerControlRestore(bool tcp);
    void InstallNativeOnlyBackend(FileStorage& storage,
                                  const FileStorageConfig& config) {
        struct NativeSource final : ssd_offload::ReadSource {
            explicit NativeSource(
                std::shared_ptr<ssd_offload::ReadSource> source)
                : source(std::move(source)) {}
            ssd_offload::ReadSourceInfo info() const noexcept override {
                return source->info();
            }
            std::optional<int> native_read_fd() const noexcept override {
                return source->native_read_fd();
            }
            ssd_offload::ReadResult ReadAt(std::span<std::byte>,
                                           uint64_t) noexcept override {
                return {0, ssd_offload::Error::kIoError};  // Must use actual
                                                           // kernel CQEs.
            }
            std::shared_ptr<ssd_offload::ReadSource> source;
        };
        struct NativeBackend final : BucketStorageBackend {
            using BucketStorageBackend::BucketStorageBackend;
            tl::expected<std::shared_ptr<ssd_offload::ReadSource>, ErrorCode>
            AcquireReadSource(const std::string& key, uint64_t bytes) override {
                auto source =
                    BucketStorageBackend::AcquireReadSource(key, bytes);
                if (!source) return tl::unexpected(source.error());
                return std::shared_ptr<ssd_offload::ReadSource>(
                    std::make_shared<NativeSource>(*source));
            }
        };
        storage.storage_backend_ = std::make_shared<NativeBackend>(
            config, BucketBackendConfig::FromEnvironment());
    }
    void SetUp() override {
        google::InitGoogleLogging("FileStorageTest");
        FLAGS_logtostderr = true;
        UnsetEnv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR");
        UnsetEnv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH");
        UnsetEnv("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES");
        UnsetEnv("MC_STORE_PINNED_RESTORE_ARENA_SIZE_BYTES");
        UnsetEnv("MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT");
        UnsetEnv("MOONCAKE_SCANMETA_ITERATOR_KEYS_LIMIT");
        UnsetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT");
        UnsetEnv("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES");
        UnsetEnv("MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT");
        UnsetEnv("MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES");
        UnsetEnv("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS");
        UnsetEnv("MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS");
        UnsetEnv("MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS");
        UnsetEnv("MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION");
        UnsetEnv("MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO");
        UnsetEnv("MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO");
        UnsetEnv("MOONCAKE_DISK_EVICTION_HIGH_WATERMARK_RATIO");
        UnsetEnv("MOONCAKE_DISK_EVICTION_LOW_WATERMARK_RATIO");
        UnsetEnv("MOONCAKE_OFFLOAD_USE_URING");
        UnsetEnv("MOONCAKE_OFFLOAD_LOCAL_STREAMING_READ");
        UnsetEnv("MOONCAKE_USE_URING");
        UnsetEnv("MOONCAKE_DISTRIBUTED_FS_TYPE");
        UnsetEnv("MOONCAKE_DFS_FS_ADAPTER");
        UnsetEnv("MOONCAKE_DISTRIBUTED_ROOT_DIR");
        UnsetEnv("MOONCAKE_DFS_ROOT_DIR");
        data_path = (std::filesystem::current_path() / "file_storage_test_data")
                        .string();
        fs::create_directories(data_path);
        for (const auto& entry : fs::directory_iterator(data_path)) {
            std::error_code ec;
            if (entry.is_regular_file()) {
                fs::remove(entry.path(), ec);
            } else if (entry.is_directory()) {
                fs::remove_all(entry.path(), ec);
            }
        }
    }

    tl::expected<void, ErrorCode> FileStorageBatchOffload(
        FileStorage& fileStorage, std::vector<std::string>& keys,
        std::vector<int64_t>& sizes,
        std::unordered_map<std::string, std::string>& batch_data) {
        std::vector<int64_t> buckets;
        return BatchOffloadUtil(*fileStorage.storage_backend_, keys, sizes,
                                batch_data, buckets);
    }

    tl::expected<std::shared_ptr<FileStorage::AllocatedBatch>, ErrorCode>
    FileStorageAllocateBatch(FileStorage& fileStorage,
                             const std::vector<std::string>& keys,
                             const std::vector<int64_t>& sizes) {
        return fileStorage.AllocateBatch(keys, sizes,
                                         *fileStorage.client_buffer_allocator_);
    }

    void SetPinnedRestoreArena(FileStorage& fileStorage, void* address,
                               size_t size) {
        fileStorage.pinned_restore_arena_allocator_ =
            ClientBufferAllocator::create(address, size);
    }

    tl::expected<void, ErrorCode> FileStorageBatchLoad(
        FileStorage& fileStorage,
        std::unordered_map<std::string, Slice>& batch_object) {
        return fileStorage.BatchLoad(batch_object);
    }

    void PrepareStreamingObject(FileStorage& storage, const std::string& key,
                                std::string& value) {
        ASSERT_TRUE(storage.storage_backend_->Init());
        ASSERT_TRUE(storage.storage_backend_->BatchOffload(
            {{key, {{value.data(), value.size()}}}},
            [](const auto&, auto&) { return ErrorCode::OK; }));
    }

    void PrepareStreamingBatch(
        FileStorage& storage,
        const std::unordered_map<std::string, std::vector<Slice>>& values) {
        ASSERT_TRUE(storage.storage_backend_->Init());
        std::vector<int64_t> buckets;
        for (const auto& [key, slices] : values) {
            auto result = storage.storage_backend_->BatchOffload(
                {{key, slices}},
                [](const auto&, auto&) { return ErrorCode::OK; });
            ASSERT_TRUE(result);
            EXPECT_EQ(std::count(buckets.begin(), buckets.end(), *result), 0);
            buckets.push_back(*result);
        }
    }

    static FileStorageConfig OwnerConfig(const std::string& path) {
        FileStorageConfig config;
        config.storage_filepath = path;
        config.local_buffer_size = 512 * 1024;
        config.streaming_read.push_enabled = true;
        config.streaming_read.slot_bytes = 64 * 1024;
        config.streaming_read.slots = 1;
        return config;
    }
    static ssd_offload::SsdReadRequestV1 OwnerRequest(const std::string& key,
                                                      size_t object_size,
                                                      size_t length,
                                                      size_t offset = 0) {
        using namespace ssd_offload;
        const PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
        const DestinationRegion region{requester, 1, 1, {7, 8}};
        return {1,
                requester,
                {owner, 1, 1},
                "owner-tenant",
                5000,
                0,
                {{key, object_size, {}, {{offset, {{region, 0, length}}}}}}};
    }

    std::optional<BufferHandle> ReserveStreamingArena(FileStorage& storage,
                                                      size_t bytes) {
        return storage.client_buffer_allocator_->allocate(bytes);
    }

    std::shared_ptr<void> RegisterStreamingArena(
        std::shared_ptr<FileStorage> storage,
        std::shared_ptr<TransferEngine> engine) {
        void* base = storage->client_buffer_allocator_->getBase();
        if (engine->registerLocalMemory(
                base, storage->client_buffer_allocator_->size(), "cpu:0", true,
                true) != 0)
            return {};
        return std::shared_ptr<void>(
            base, [storage = std::move(storage),
                   engine = std::move(engine)](void* pointer) {
                EXPECT_EQ(engine->unregisterLocalMemory(pointer, true), 0);
            });
    }

    tl::expected<bool, ErrorCode> FileStorageIsEnableOffloading(
        FileStorage& fileStorage) {
        return fileStorage.IsEnableOffloading();
    }

    bool SubmitStreamingReadTask(FileStorage& storage,
                                 std::function<void()> task) {
        return storage.streaming_reads_->TrySubmit(std::move(task));
    }
    size_t ActiveStreamingSources(FileStorage& storage) {
        return storage.active_streaming_sources_.load(
            std::memory_order_relaxed);
    }

    bool FileStorageUsesDfsControlPlane(const FileStorage& file_storage) {
        return file_storage.config_.enable_dfs;
    }

    tl::expected<void, ErrorCode> FileStorageNotifyEvictedDiskReplicas(
        FileStorage& fileStorage,
        const std::vector<std::string>& evicted_keys) {
        return fileStorage.NotifyEvictedDiskReplicas(evicted_keys);
    }

    tl::expected<void, ErrorCode> FileStorageGroupOffloadingKeysByBucket(
        FileStorage& fileStorage,
        const std::unordered_map<std::string, int64_t>& offloading_objects,
        std::vector<std::vector<std::string>>& buckets_keys) {
        auto bucket_backend = std::dynamic_pointer_cast<BucketStorageBackend>(
            fileStorage.storage_backend_);
        if (!bucket_backend) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return bucket_backend->AllocateOffloadingBuckets(offloading_objects,
                                                         buckets_keys);
    }

    size_t GetUngroupedOffloadingObjectsSize(FileStorage& fileStorage) {
        auto bucket_backend = std::dynamic_pointer_cast<BucketStorageBackend>(
            fileStorage.storage_backend_);
        if (!bucket_backend) {
            return 0;
        }
        return bucket_backend->UngroupedOffloadingObjectsSize();
    }

    // Static funnel to the private FileStorage::IsPerBucketSoftOffloadError.
    // FileStorageTest is friended; TEST_F-generated subclasses are not.
    static bool CallIsPerBucketSoftOffloadError(ErrorCode error) {
        return FileStorage::IsPerBucketSoftOffloadError(error);
    }

    // Funnel to the private FileStorage::Heartbeat, for the drain tests.
    static tl::expected<void, ErrorCode> FileStorageHeartbeat(
        FileStorage& fileStorage) {
        return fileStorage.Heartbeat();
    }

    // Drives the drain-vs-heartbeat interleaving deterministically: a
    // heartbeat tick that has already passed its entry draining_ check parks
    // on offloading_mutex_, a drain runs, and the parked tick resumes only
    // around the drain's own critical section. The fixture holds the mutex
    // as the scheduler while both threads are launched, so the tick is
    // provably not inside its RPC section when the drain latches. After the
    // release the two race for the lock; the postcondition (segment
    // deregistered, never re-mounted) must hold in both wake orders, so the
    // assertions do not depend on the schedule. Without the drain taking
    // offloading_mutex_, the parked tick would resume after the drain
    // finished, get SEGMENT_NOT_FOUND, and re-mount the segment.
    tl::expected<void, ErrorCode> DrainWhileHeartbeatTickParked(
        FileStorage& fileStorage) {
        // The parked tick may win the lock and run a full pass, which is
        // only supported on an initialized backend (Init() is also what
        // starts the heartbeat thread in production).
        EXPECT_TRUE(fileStorage.storage_backend_->Init());
        tl::expected<void, ErrorCode> drain_result =
            tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        std::thread heartbeat_thread;
        std::thread drain_thread;
        {
            MutexLocker scheduler(&fileStorage.offloading_mutex_);
            heartbeat_thread =
                std::thread([&fileStorage] { fileStorage.Heartbeat(); });
            // Let the tick pass the entry check and park on the mutex we
            // hold. The assertions do not depend on it parking -- a tick
            // that has not reached the lock is caught by the entry check
            // instead -- the pause just pins the adversarial schedule.
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            drain_thread = std::thread([&fileStorage, &drain_result] {
                drain_result = fileStorage.DrainLocalDiskSegment(0);
            });
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        heartbeat_thread.join();
        drain_thread.join();
        return drain_result;
    }

    void AssertHeartbeatEvictsAllKeys(
        FileStorage& fileStorage, const std::vector<std::string>& expected_keys,
        const std::unordered_map<std::string, std::vector<Slice>>&
            batch_object) {
        ASSERT_TRUE(fileStorage.storage_backend_->Init());
        {
            MutexLocker locker(&fileStorage.offloading_mutex_);
            fileStorage.enable_offloading_ = true;
        }

        auto offload_result = fileStorage.storage_backend_->BatchOffload(
            batch_object,
            [&fileStorage](const std::vector<std::string>& keys,
                           std::vector<StorageObjectMetadata>& metadatas) {
                for (auto& metadata : metadatas) {
                    metadata.transport_endpoint = fileStorage.local_rpc_addr_;
                }
                auto result =
                    fileStorage.client_->NotifyOffloadSuccess(keys, metadatas);
                if (!result) {
                    return result.error();
                }
                return ErrorCode::OK;
            });
        ASSERT_TRUE(offload_result.has_value());
        ASSERT_EQ(offload_result.value(),
                  static_cast<int64_t>(expected_keys.size()));

        for (const auto& key : expected_keys) {
            auto query_result = fileStorage.client_->Query(key);
            ASSERT_TRUE(query_result.has_value());
            bool has_local_disk_replica = false;
            for (const auto& replica : query_result->replicas) {
                has_local_disk_replica |= replica.is_local_disk_replica();
            }
            EXPECT_TRUE(has_local_disk_replica);
        }

        auto heartbeat_result = fileStorage.Heartbeat();
        ASSERT_TRUE(heartbeat_result.has_value());

        for (const auto& key : expected_keys) {
            auto exists = fileStorage.storage_backend_->IsExist(key);
            ASSERT_TRUE(exists.has_value());
            EXPECT_FALSE(exists.value());

            auto query_result = fileStorage.client_->Query(key);
            ASSERT_FALSE(query_result.has_value());
            EXPECT_EQ(query_result.error(), ErrorCode::OBJECT_NOT_FOUND);
        }
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
        LOG(INFO) << "Clear test data...";
        for (const auto& entry : fs::directory_iterator(data_path)) {
            if (entry.is_regular_file()) {
                fs::remove(entry.path());
            }
        }
    }
    void RunPutHealWipeScenario(const std::string& data_path,
                                StorageBackendType backend_type,
                                const std::string& suffix) {
        std::filesystem::path master_root =
            std::filesystem::path(data_path) / ("heal_master" + suffix);
        std::filesystem::create_directories(master_root);

        testing::InProcMaster master;
        auto master_config = InProcMasterConfigBuilder()
                                 .set_enable_offload(true)
                                 .set_root_fs_dir(master_root.string())
                                 .build();
        ASSERT_TRUE(master.Start(master_config));

        std::string local_rpc_addr =
            "127.0.0.1:" + std::to_string(getFreeTcpPort());
        auto client =
            Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                           std::nullopt, master.master_address());
        ASSERT_TRUE(client.has_value());
        ASSERT_TRUE(client.value()->MountLocalDiskSegment(true).has_value());

        // Memory segment and registered buffers, so the retried PutStart can
        // allocate a memory replica and Get can read it back.
        constexpr size_t kSegSize = 64 * 1024 * 1024;
        void* seg_ptr = allocate_buffer_allocator_memory(kSegSize);
        ASSERT_NE(seg_ptr, nullptr);
        ASSERT_TRUE(
            client.value()->MountSegment(seg_ptr, kSegSize, "tcp").has_value());
        SimpleAllocator allocator(16 * 1024 * 1024);
        ASSERT_TRUE(client.value()
                        ->RegisterLocalMemory(allocator.getBase(),
                                              16 * 1024 * 1024, "cpu:0", false,
                                              false)
                        .has_value());

        FileStorageConfig config = FileStorageConfig::FromEnvironment();
        config.storage_backend_type = backend_type;
        config.storage_filepath = data_path + "/heal_ssd" + suffix;
        config.local_buffer_size = 4 * 1024 * 1024;
        fs::create_directories(config.storage_filepath);
        FileStorage file_storage(config, client.value(), local_rpc_addr);
        ASSERT_TRUE(file_storage.storage_backend_->Init());
        {
            MutexLocker locker(&file_storage.offloading_mutex_);
            file_storage.enable_offloading_ = true;
        }

        // Offload one key so master tracks a LOCAL_DISK replica backed by a
        // real file on the client SSD.
        const std::string key = "heal_key" + suffix;
        std::string original(512, 'x');
        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        batch_object.emplace(key,
                             std::vector<Slice>{Slice{original.data(), 512}});
        auto offload_result = file_storage.storage_backend_->BatchOffload(
            batch_object,
            [&file_storage](const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas) {
                for (auto& metadata : metadatas) {
                    metadata.transport_endpoint = file_storage.local_rpc_addr_;
                }
                auto result =
                    file_storage.client_->NotifyOffloadSuccess(keys, metadatas);
                return result ? ErrorCode::OK : result.error();
            });
        ASSERT_TRUE(offload_result.has_value());

        // Wire the probe the way RealClient does in production: existence
        // against this process's offload files.
        client.value()->SetLocalDiskProbe(
            [&file_storage](const std::string& k) -> std::optional<bool> {
                auto exists = file_storage.Exists(k);
                if (!exists) {
                    return std::nullopt;
                }
                return !*exists;
            });

        auto query = client.value()->Query(key);
        ASSERT_TRUE(query.has_value());
        bool has_local_disk = false;
        for (const auto& replica : query->replicas) {
            has_local_disk |= replica.is_local_disk_replica();
        }
        ASSERT_TRUE(has_local_disk);

        // Simulate the #3709 divergence: the SSD file is wiped physically while
        // master keeps the metadata.
        file_storage.storage_backend_->RemoveAll();
        auto exists = file_storage.Exists(key);
        ASSERT_TRUE(exists.has_value());
        ASSERT_FALSE(exists.value());
        // QueryResult is not assignable, so re-query into a fresh variable.
        auto query_after_wipe = client.value()->Query(key);
        ASSERT_TRUE(query_after_wipe.has_value());

        // Put the same key again: the dangling replica must be evicted and the
        // Put must really store new data.
        void* buf = allocator.allocate(512);
        ASSERT_NE(buf, nullptr);
        std::string updated(512, 'y');
        std::memcpy(buf, updated.data(), 512);
        std::vector<Slice> slices{Slice{buf, 512}};
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        auto put = client.value()->Put(key, slices, cfg);
        ASSERT_TRUE(put.has_value())
            << "Put over a dangling replica failed: " << toString(put.error());
        allocator.deallocate(buf, 512);

        // The new data is readable through the normal Get path.
        void* read_buf = allocator.allocate(512);
        ASSERT_NE(read_buf, nullptr);
        std::vector<Slice> read_slices{Slice{read_buf, 512}};
        auto get = client.value()->Get(key, read_slices);
        ASSERT_TRUE(get.has_value())
            << "Get after self-heal failed: " << toString(get.error());
        EXPECT_EQ(std::memcmp(read_slices[0].ptr, updated.data(), 512), 0);
        allocator.deallocate(read_buf, 512);

        auto unmount = client.value()->UnmountSegment(seg_ptr, kSegSize);
        EXPECT_TRUE(unmount.has_value());
        std::free(seg_ptr);
    }
    void RunBatchPutHealWipeScenario(const std::string& data_path) {
        std::filesystem::path master_root =
            std::filesystem::path(data_path) / "heal_master_batch";
        std::filesystem::create_directories(master_root);

        testing::InProcMaster master;
        auto master_config = InProcMasterConfigBuilder()
                                 .set_enable_offload(true)
                                 .set_root_fs_dir(master_root.string())
                                 .build();
        ASSERT_TRUE(master.Start(master_config));

        std::string local_rpc_addr =
            "127.0.0.1:" + std::to_string(getFreeTcpPort());
        auto client =
            Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                           std::nullopt, master.master_address());
        ASSERT_TRUE(client.has_value());
        ASSERT_TRUE(client.value()->MountLocalDiskSegment(true).has_value());

        constexpr size_t kSegSize = 64 * 1024 * 1024;
        void* seg_ptr = allocate_buffer_allocator_memory(kSegSize);
        ASSERT_NE(seg_ptr, nullptr);
        ASSERT_TRUE(
            client.value()->MountSegment(seg_ptr, kSegSize, "tcp").has_value());
        SimpleAllocator allocator(16 * 1024 * 1024);
        ASSERT_TRUE(client.value()
                        ->RegisterLocalMemory(allocator.getBase(),
                                              16 * 1024 * 1024, "cpu:0", false,
                                              false)
                        .has_value());

        FileStorageConfig config = FileStorageConfig::FromEnvironment();
        config.storage_backend_type = StorageBackendType::kFilePerKey;
        config.storage_filepath = data_path + "/heal_ssd_batch";
        config.local_buffer_size = 4 * 1024 * 1024;
        fs::create_directories(config.storage_filepath);
        FileStorage file_storage(config, client.value(), local_rpc_addr);
        ASSERT_TRUE(file_storage.storage_backend_->Init());
        {
            MutexLocker locker(&file_storage.offloading_mutex_);
            file_storage.enable_offloading_ = true;
        }

        // Offload two keys, then wipe the backing files to recreate the #3709
        // divergence on both.
        const std::vector<std::string> wiped_keys = {"heal_batch_a",
                                                     "heal_batch_b"};
        std::string original(512, 'x');
        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        for (const auto& key : wiped_keys) {
            batch_object.emplace(
                key, std::vector<Slice>{Slice{original.data(), 512}});
        }
        auto offload_result = file_storage.storage_backend_->BatchOffload(
            batch_object,
            [&file_storage](const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas) {
                for (auto& metadata : metadatas) {
                    metadata.transport_endpoint = file_storage.local_rpc_addr_;
                }
                auto result =
                    file_storage.client_->NotifyOffloadSuccess(keys, metadatas);
                return result ? ErrorCode::OK : result.error();
            });
        ASSERT_TRUE(offload_result.has_value());

        client.value()->SetLocalDiskProbe(
            [&file_storage](const std::string& k) -> std::optional<bool> {
                auto exists = file_storage.Exists(k);
                if (!exists) {
                    return std::nullopt;
                }
                return !*exists;
            });

        file_storage.storage_backend_->RemoveAll();
        auto exists = file_storage.Exists(wiped_keys[0]);
        ASSERT_TRUE(exists.has_value());
        ASSERT_FALSE(exists.value());

        // One batch with both wiped keys and a fresh key.
        const std::string fresh_key = "heal_batch_fresh";
        const std::string updated(512, 'y');
        std::vector<ObjectKey> keys = {wiped_keys[0], wiped_keys[1], fresh_key};
        std::vector<std::vector<Slice>> batched_slices;
        std::vector<void*> buffers;
        for (size_t i = 0; i < keys.size(); ++i) {
            void* buf = allocator.allocate(512);
            ASSERT_NE(buf, nullptr);
            std::memcpy(buf, updated.data(), 512);
            buffers.push_back(buf);
            batched_slices.push_back({Slice{buf, 512}});
        }
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        auto results = client.value()->BatchPut(keys, batched_slices, cfg);
        ASSERT_EQ(results.size(), keys.size());
        for (size_t i = 0; i < results.size(); ++i) {
            ASSERT_TRUE(results[i].has_value())
                << "BatchPut over a dangling replica failed for " << keys[i]
                << ": " << toString(results[i].error());
        }

        // Every key, healed or fresh, reads the new data back.
        for (const auto& key : keys) {
            void* read_buf = allocator.allocate(512);
            ASSERT_NE(read_buf, nullptr);
            std::vector<Slice> read_slices{Slice{read_buf, 512}};
            auto get = client.value()->Get(key, read_slices);
            ASSERT_TRUE(get.has_value())
                << "Get after batch self-heal failed for " << key << ": "
                << toString(get.error());
            EXPECT_EQ(std::memcmp(read_slices[0].ptr, updated.data(), 512), 0);
            allocator.deallocate(read_buf, 512);
        }
        for (void* buf : buffers) {
            allocator.deallocate(buf, 512);
        }

        auto unmount = client.value()->UnmountSegment(seg_ptr, kSegSize);
        EXPECT_TRUE(unmount.has_value());
        std::free(seg_ptr);
    }
};

TEST_F(FileStorageTest, SkippedOffloadAllDuplicatesReleaseSourceTasks) {
    RunSkippedOffloadScenario(2, false);
}

TEST_F(FileStorageTest, SkippedOffloadMixedBatchReleasesSourceTasks) {
    RunSkippedOffloadScenario(1, false);
}

TEST_F(FileStorageTest, SkippedOffloadFreshBatchKeepsSuccessNotifications) {
    RunSkippedOffloadScenario(0, false);
}

TEST_F(FileStorageTest, SkippedOffloadPartialFileWriteReleasesSourceTasks) {
    RunSkippedOffloadScenario(1, false, StorageBackendType::kFilePerKey);
}

TEST_F(FileStorageTest, SkippedOffloadAllFileWritesFailReleasesSourceTasks) {
    RunSkippedOffloadScenario(2, false, StorageBackendType::kFilePerKey);
}

TEST_F(FileStorageTest, SkippedOffloadWriteFailureReleasesAllSourceTasks) {
    RunSkippedOffloadScenario(1, true);
}

TEST_F(FileStorageTest, IsEnableOffloading) {
    std::unordered_map<std::string, std::string> all_object;
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    FileStorage fileStorage1(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage1, keys, sizes, batch_data));
    auto enable_offloading_result1 =
        FileStorageIsEnableOffloading(fileStorage1);
    ASSERT_TRUE(enable_offloading_result1 && enable_offloading_result1.value());

    // bucket_keys_limit/bucket_size_limit moved to BucketBackendConfig.
    // With current semantics, backend prevents offloading once it would exceed
    // limits, so we validate IsEnableOffloading directly under tight limits.

    // Case 2: total_keys_limit < bucket_keys_limit => cannot offload
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "10");
    file_storage_config.total_keys_limit = 9;
    FileStorage fileStorage2(file_storage_config, nullptr, "localhost:9003");
    auto enable_offloading_result2 =
        FileStorageIsEnableOffloading(fileStorage2);
    ASSERT_TRUE(enable_offloading_result2 &&
                !enable_offloading_result2.value());

    // Case 3: total_size_limit < bucket_size_limit => cannot offload
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES", "969");
    file_storage_config.total_keys_limit = 10'000'000;
    file_storage_config.total_size_limit = 100;
    FileStorage fileStorage3(file_storage_config, nullptr, "localhost:9003");
    auto enable_offloading_result3 =
        FileStorageIsEnableOffloading(fileStorage3);
    ASSERT_TRUE(enable_offloading_result3 &&
                !enable_offloading_result3.value());
}

TEST_F(FileStorageTest, DistributedBackendSelectsControlPlaneFromStorageMode) {
    SetEnv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
           "distributed_storage_backend");
    SetEnv("MOONCAKE_DISTRIBUTED_ROOT_DIR", data_path + "/distributed");

    auto config = FileStorageConfig::FromEnvironment();
    config.storage_filepath = data_path;
    config.local_buffer_size = 4 * 1024 * 1024;
    EXPECT_FALSE(config.enable_dfs);

    SetEnv("MOONCAKE_DISTRIBUTED_FS_TYPE", "posix");
    {
        FileStorage file_storage(config, nullptr, "localhost:9003");
        EXPECT_TRUE(FileStorageUsesDfsControlPlane(file_storage));
    }

#ifdef HAVE_OSS_ADAPTER
    SetEnv("MOONCAKE_DISTRIBUTED_FS_TYPE", "oss");
    FileStorage file_storage(config, nullptr, "localhost:9003");
    EXPECT_FALSE(FileStorageUsesDfsControlPlane(file_storage));

    auto enable_offloading = FileStorageIsEnableOffloading(file_storage);
    ASSERT_TRUE(enable_offloading);
    EXPECT_TRUE(*enable_offloading);
#endif
}

TEST_F(FileStorageTest, BatchGetUsesPinnedArenaAndFallsBackWhenFull) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));

    size_t payload_size = 0;
    for (const auto size : sizes) {
        payload_size += static_cast<size_t>(size);
    }
    const size_t arena_size = payload_size * 3 / 2;
    std::vector<char> restore_arena(arena_size + 4096);
    const auto arena_begin =
        (reinterpret_cast<uintptr_t>(restore_arena.data()) + 4095) & ~4095ULL;
    SetPinnedRestoreArena(fileStorage, reinterpret_cast<void*>(arena_begin),
                          arena_size);

    auto pinned_result = fileStorage.BatchGetLocal(keys, sizes);
    ASSERT_TRUE(pinned_result);
    ASSERT_EQ(pinned_result->pointers.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        EXPECT_GE(pinned_result->pointers[i], arena_begin);
        EXPECT_LT(pinned_result->pointers[i], arena_begin + arena_size);
        EXPECT_EQ(
            std::string(reinterpret_cast<char*>(pinned_result->pointers[i]),
                        sizes[i]),
            batch_data.at(keys[i]));
    }

    auto fallback_result = fileStorage.BatchGetLocal(keys, sizes);
    ASSERT_TRUE(fallback_result);
    ASSERT_EQ(fallback_result->pointers.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        EXPECT_TRUE(fallback_result->pointers[i] < arena_begin ||
                    fallback_result->pointers[i] >= arena_begin + arena_size);
        EXPECT_EQ(
            std::string(reinterpret_cast<char*>(fallback_result->pointers[i]),
                        sizes[i]),
            batch_data.at(keys[i]));
    }
}

TEST_F(FileStorageTest, AllocateBatchAvoidsDirectIoPaddingForPosixReads) {
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 64 * 1024;
    file_storage_config.use_uring = false;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");

    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    for (size_t i = 0; i < 16; ++i) {
        keys.emplace_back("key" + std::to_string(i));
        sizes.emplace_back(4 * 1024);
    }

    auto allocate_result = FileStorageAllocateBatch(fileStorage, keys, sizes);
    ASSERT_TRUE(allocate_result)
        << "POSIX reads should not reserve O_DIRECT padding";
    EXPECT_EQ(allocate_result.value()->total_size,
              file_storage_config.local_buffer_size);
    ASSERT_EQ(allocate_result.value()->handles.size(), keys.size());
    for (const auto& handle : allocate_result.value()->handles) {
        EXPECT_EQ(handle.size(), 4 * 1024);
    }
}

TEST_F(FileStorageTest, GroupOffloadingKeysByBucket_bucket_keys_limit) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    for (size_t i = 0; i < 35; i++) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.scanmeta_iterator_keys_limit = 969;
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "10");
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 3);
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
    }
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 5);
    for (size_t i = 35; i < 40; ++i) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    buckets_keys.clear();
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 4);
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
    }
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 0);
}

TEST_F(FileStorageTest, GroupOffloadingKeysByBucket_deduplicates_carryover) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    offloading_objects.emplace("duplicate", 1);

    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "10");
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");

    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_TRUE(buckets_keys.empty());
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 1);

    for (size_t i = 0; i < 9; ++i) {
        offloading_objects.emplace("new" + std::to_string(i), 1);
    }
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));

    ASSERT_EQ(buckets_keys.size(), 1);
    ASSERT_EQ(buckets_keys.front().size(), 10);
    std::unordered_set<std::string> unique_keys(buckets_keys.front().begin(),
                                                buckets_keys.front().end());
    EXPECT_EQ(unique_keys.size(), 10);
    EXPECT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 0);
}

TEST_F(FileStorageTest, GroupOffloadingKeysByBucket_bucket_size_limit) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    for (size_t i = 0; i < 35; i++) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES", "10");
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 3);
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
    }
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 5);
    for (size_t i = 35; i < 40; ++i) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    buckets_keys.clear();
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 4);
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
    }
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 0);
}

TEST_F(FileStorageTest,
       GroupOffloadingKeysByBucket_bucket_size_limit_and_bucket_keys_limit) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    for (size_t i = 0; i < 500; i++) {
        offloading_objects.emplace("test" + std::to_string(i), i);
    }
    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "9");
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES", "496");
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    for (size_t i = 0; i < buckets_keys.size(); i++) {
        auto bucket_keys = buckets_keys.at(i);
        ASSERT_TRUE(bucket_keys.size() <= 9);
        size_t total_size = 0;
        std::string keys;
        for (const auto& bucket_key : bucket_keys) {
            total_size += offloading_objects.at(bucket_key);
            keys += bucket_key + ",";
        }
        ASSERT_TRUE(total_size <= 496);
    }
}

TEST_F(FileStorageTest,
       GroupOffloadingKeysByBucket_ungrouped_offloading_objects) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    for (size_t i = 0; i < 1; i++) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    offloading_objects.clear();
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    for (size_t i = 0; i < 7; i++) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
}

TEST_F(FileStorageTest, HeartbeatRunsDiskWatermarkEvictionWithoutOffloadWork) {
    std::filesystem::path master_root =
        std::filesystem::path(data_path) / "heartbeat_master";
    std::filesystem::create_directories(master_root);

    testing::InProcMaster master;
    auto master_config = InProcMasterConfigBuilder()
                             .set_enable_offload(true)
                             .set_root_fs_dir(master_root.string())
                             .build();
    ASSERT_TRUE(master.Start(master_config));

    std::string local_rpc_addr =
        "127.0.0.1:" + std::to_string(getFreeTcpPort());
    auto client = Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                                 std::nullopt, master.master_address());
    ASSERT_TRUE(client.has_value());
    auto mount_result = client.value()->MountLocalDiskSegment(true);
    ASSERT_TRUE(mount_result.has_value())
        << "MountLocalDiskSegment failed: " << toString(mount_result.error());

    FileStorageConfig config = FileStorageConfig::FromEnvironment();
    config.storage_backend_type = StorageBackendType::kFilePerKey;
    config.storage_filepath = data_path + "/heartbeat_watermark";
    config.local_buffer_size = 4 * 1024 * 1024;
    config.disk_eviction_high_watermark_ratio = 1e-12;
    config.disk_eviction_low_watermark_ratio = 0.5e-12;
    fs::create_directories(config.storage_filepath);

    FileStorage file_storage(config, client.value(), local_rpc_addr);

    std::unordered_map<std::string, std::vector<Slice>> batch_object;
    std::vector<std::unique_ptr<char[]>> buffers;
    std::vector<std::string> expected_keys = {
        "heartbeat_key_1", "heartbeat_key_2", "heartbeat_key_3"};
    for (size_t i = 0; i < expected_keys.size(); ++i) {
        auto buffer = std::make_unique<char[]>(512);
        std::memset(buffer.get(), static_cast<int>('a' + i), 512);
        batch_object.emplace(expected_keys[i],
                             std::vector<Slice>{Slice{buffer.get(), 512}});
        buffers.push_back(std::move(buffer));
    }

    AssertHeartbeatEvictsAllKeys(file_storage, expected_keys, batch_object);
}

// A drain deregisters the disk tier; the heartbeat re-mounts the segment
// whenever the master answers SEGMENT_NOT_FOUND. These pin the interleavings
// where the two overlap: the drain must win in every schedule.

TEST_F(FileStorageTest, HeartbeatAfterDrainDoesNotRemount) {
    std::filesystem::path master_root =
        std::filesystem::path(data_path) / "drain_hb_master";
    std::filesystem::create_directories(master_root);

    testing::InProcMaster master;
    auto master_config = InProcMasterConfigBuilder()
                             .set_enable_offload(true)
                             .set_root_fs_dir(master_root.string())
                             .build();
    ASSERT_TRUE(master.Start(master_config));

    std::string local_rpc_addr =
        "127.0.0.1:" + std::to_string(getFreeTcpPort());
    auto client = Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                                 std::nullopt, master.master_address());
    ASSERT_TRUE(client.has_value());
    ASSERT_TRUE(client.value()->MountLocalDiskSegment(true).has_value());

    FileStorageConfig config = FileStorageConfig::FromEnvironment();
    config.storage_backend_type = StorageBackendType::kFilePerKey;
    config.storage_filepath = data_path + "/drain_hb";
    config.local_buffer_size = 4 * 1024 * 1024;
    fs::create_directories(config.storage_filepath);
    FileStorage file_storage(config, client.value(), local_rpc_addr);

    ASSERT_TRUE(file_storage.DrainLocalDiskSegment(0).has_value());

    // A tick after the drain is a no-op: it must not take the
    // SEGMENT_NOT_FOUND recovery branch and re-mount the segment.
    ASSERT_TRUE(FileStorageHeartbeat(file_storage).has_value());

    std::vector<OffloadTaskItem> offload_items;
    auto heartbeat_rpc =
        client.value()->OffloadObjectHeartbeat(true, offload_items);
    ASSERT_FALSE(heartbeat_rpc.has_value());
    EXPECT_EQ(ErrorCode::SEGMENT_NOT_FOUND, heartbeat_rpc.error());
}

TEST_F(FileStorageTest, DrainSurvivesParkedHeartbeatTick) {
    std::filesystem::path master_root =
        std::filesystem::path(data_path) / "drain_race_master";
    std::filesystem::create_directories(master_root);

    testing::InProcMaster master;
    auto master_config = InProcMasterConfigBuilder()
                             .set_enable_offload(true)
                             .set_root_fs_dir(master_root.string())
                             .build();
    ASSERT_TRUE(master.Start(master_config));

    std::string local_rpc_addr =
        "127.0.0.1:" + std::to_string(getFreeTcpPort());
    auto client = Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                                 std::nullopt, master.master_address());
    ASSERT_TRUE(client.has_value());
    ASSERT_TRUE(client.value()->MountLocalDiskSegment(true).has_value());

    FileStorageConfig config = FileStorageConfig::FromEnvironment();
    config.storage_backend_type = StorageBackendType::kFilePerKey;
    config.storage_filepath = data_path + "/drain_race";
    config.local_buffer_size = 4 * 1024 * 1024;
    fs::create_directories(config.storage_filepath);
    FileStorage file_storage(config, client.value(), local_rpc_addr);

    // A tick that passed its entry draining_ check is parked on
    // offloading_mutex_ while the drain runs (see the fixture helper). It
    // must not resume into the SEGMENT_NOT_FOUND recovery branch and
    // re-mount the segment the drain deregistered.
    auto drain_result = DrainWhileHeartbeatTickParked(file_storage);
    ASSERT_TRUE(drain_result.has_value());

    std::vector<OffloadTaskItem> offload_items;
    auto heartbeat_rpc =
        client.value()->OffloadObjectHeartbeat(true, offload_items);
    ASSERT_FALSE(heartbeat_rpc.has_value());
    EXPECT_EQ(ErrorCode::SEGMENT_NOT_FOUND, heartbeat_rpc.error());
}

TEST_F(FileStorageTest, NotifyEvictedDiskReplicasUsesTenantScopedKeys) {
    std::filesystem::path master_root =
        std::filesystem::path(data_path) / "tenant_notify_master";
    std::filesystem::create_directories(master_root);

    testing::InProcMaster master;
    auto master_config = InProcMasterConfigBuilder()
                             .set_enable_offload(true)
                             .set_root_fs_dir(master_root.string())
                             .build();
    ASSERT_TRUE(master.Start(master_config));

    std::string local_rpc_addr =
        "127.0.0.1:" + std::to_string(getFreeTcpPort());
    auto client = Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                                 std::nullopt, master.master_address());
    ASSERT_TRUE(client.has_value());
    auto mount_result = client.value()->MountLocalDiskSegment(true);
    ASSERT_TRUE(mount_result.has_value())
        << "MountLocalDiskSegment failed: " << toString(mount_result.error());

    FileStorageConfig config = FileStorageConfig::FromEnvironment();
    config.storage_backend_type = StorageBackendType::kFilePerKey;
    config.storage_filepath = data_path + "/tenant_notify";
    fs::create_directories(config.storage_filepath);
    FileStorage file_storage(config, client.value(), local_rpc_addr);

    const std::string key = "shared_key";
    std::vector<OffloadTaskItem> tasks = {
        {.tenant_id = "tenant_a", .key = key, .size = 128},
        {.tenant_id = "tenant_b", .key = key, .size = 128},
    };
    std::vector<StorageObjectMetadata> metadatas;
    metadatas.reserve(tasks.size());
    for (const auto& task : tasks) {
        metadatas.push_back(StorageObjectMetadata{
            .bucket_id = 0,
            .offset = 0,
            .key_size = static_cast<int64_t>(task.key.size()),
            .data_size = task.size,
            .transport_endpoint = local_rpc_addr,
        });
    }
    ASSERT_TRUE(client.value()->NotifyOffloadSuccess(tasks, metadatas));

    for (const auto& task : tasks) {
        auto before = client.value()->BatchQuery({key}, task.tenant_id);
        ASSERT_EQ(before.size(), 1);
        ASSERT_TRUE(before[0].has_value());
        bool has_local_disk_replica = false;
        for (const auto& replica : before[0]->replicas) {
            has_local_disk_replica |= replica.is_local_disk_replica();
        }
        ASSERT_TRUE(has_local_disk_replica);
    }

    auto notify_result = FileStorageNotifyEvictedDiskReplicas(
        file_storage, {TenantId("tenant_a").MakeScopedKey(key),
                       TenantId("tenant_b").MakeScopedKey(key)});
    ASSERT_TRUE(notify_result.has_value());

    for (const auto& task : tasks) {
        auto after = client.value()->BatchQuery({key}, task.tenant_id);
        ASSERT_EQ(after.size(), 1);
        ASSERT_FALSE(after[0].has_value());
        EXPECT_EQ(after[0].error(), ErrorCode::OBJECT_NOT_FOUND);
    }
}

// Regression test for issue #2827: under concurrent/repeat offload of the same
// keys, the bucket backend rejects a whole bucket atomically with
// OBJECT_ALREADY_EXISTS (see BucketStorageBackend duplicate-key tests). That
// error must be treated as a per-bucket soft failure so OffloadObjects reports
// the keys back to the master and continues, rather than aborting the whole
// offload cycle and leaving master/SSD metadata inconsistent (which surfaced as
// spurious INVALID_KEY on the read path). It stays alongside INVALID_READ,
// while genuinely fatal errors (e.g. KEYS_ULTRA_LIMIT, INTERNAL_ERROR) do not.
TEST_F(FileStorageTest, DuplicateOffloadErrorIsPerBucketSoftError) {
    EXPECT_TRUE(
        CallIsPerBucketSoftOffloadError(ErrorCode::OBJECT_ALREADY_EXISTS));
    EXPECT_TRUE(CallIsPerBucketSoftOffloadError(ErrorCode::INVALID_READ));

    EXPECT_FALSE(CallIsPerBucketSoftOffloadError(ErrorCode::KEYS_ULTRA_LIMIT));
    EXPECT_FALSE(CallIsPerBucketSoftOffloadError(ErrorCode::INTERNAL_ERROR));
    EXPECT_FALSE(CallIsPerBucketSoftOffloadError(ErrorCode::INVALID_KEY));
    EXPECT_FALSE(CallIsPerBucketSoftOffloadError(ErrorCode::OK));
}

TEST_F(FileStorageTest, BatchLoad_WithStorageBackendAdaptor) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_backend_type = StorageBackendType::kFilePerKey;
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    FilePerKeyConfig file_per_key_config;
    file_per_key_config.fsdir = "FileStorageTestDir";

    auto total_path = fs::path(data_path) / file_per_key_config.fsdir;
    fs::create_directories(total_path);

    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");

    auto offload_res =
        FileStorageBatchOffload(fileStorage, keys, sizes, batch_data);
    ASSERT_TRUE(offload_res) << "FileStorageBatchOffload failed";

    auto allocate_res = FileStorageAllocateBatch(fileStorage, keys, sizes);
    ASSERT_TRUE(allocate_res) << "FileStorageAllocateBatch failed";

    auto batch = std::move(allocate_res.value());

    auto load_res = FileStorageBatchLoad(fileStorage, batch->slices);
    ASSERT_TRUE(load_res) << "FileStorageBatchLoad failed";

    for (const auto& it : batch->slices) {
        const std::string& key = it.first;
        const Slice& slice = it.second;
        std::string data(static_cast<char*>(slice.ptr), slice.size);

        auto found = batch_data.find(key);
        ASSERT_TRUE(found != batch_data.end())
            << "key not found in batch_data: " << key;
        EXPECT_EQ(data, found->second);
    }
}

TEST_F(FileStorageTest, BatchLoadRecordsSsdMetrics) {
    // Setup: write data to storage backend via BatchOffloadUtil
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;

    // Create FileStorage WITH SsdMetric
    SsdMetric ssd_metric;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003",
                            &ssd_metric);

    // Write test data to disk
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());

    // Allocate buffers and call BatchLoad (read path)
    auto allocate_res = FileStorageAllocateBatch(fileStorage, keys, sizes);
    ASSERT_TRUE(allocate_res);

    auto load_result =
        FileStorageBatchLoad(fileStorage, allocate_res.value()->slices);
    ASSERT_TRUE(load_result);

    // Verify SSD read metrics were recorded
    EXPECT_EQ(ssd_metric.ssd_read_ops.value(),
              static_cast<int64_t>(allocate_res.value()->slices.size()));

    // Verify bytes: sum of all slice sizes
    int64_t expected_bytes = 0;
    for (const auto& [key, slice] : allocate_res.value()->slices) {
        expected_bytes += slice.size;
    }
    EXPECT_EQ(ssd_metric.ssd_read_bytes.value(), expected_bytes);
    EXPECT_GT(expected_bytes, 0);

    // Verify latency histogram has exactly 1 observation (one BatchLoad call)
    auto buckets = ssd_metric.ssd_read_latency_us.get_bucket_counts();
    int64_t total_observations = 0;
    for (auto& b : buckets) {
        total_observations += b->value();
    }
    EXPECT_EQ(total_observations, 1);

    // Write metrics should remain 0 (BatchOffloadUtil bypasses FileStorage)
    EXPECT_EQ(ssd_metric.ssd_write_ops.value(), 0);
    EXPECT_EQ(ssd_metric.ssd_write_bytes.value(), 0);

    LOG(INFO) << "SSD read metrics after BatchLoad: ops="
              << ssd_metric.ssd_read_ops.value()
              << ", bytes=" << ssd_metric.ssd_read_bytes.value();
}

TEST_F(FileStorageTest, BatchLoadFailureDoesNotRecordSsdMetrics) {
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;

    SsdMetric ssd_metric;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003",
                            &ssd_metric);

    // Init storage backend so we can call BatchLoad
    // But load with keys that don't exist on disk -> should fail
    std::unordered_map<std::string, Slice> batch_object;
    char dummy_buf[4096] = {};
    batch_object["nonexistent_key_1"] = Slice{dummy_buf, 4096};
    batch_object["nonexistent_key_2"] = Slice{dummy_buf, 8192};

    auto result = FileStorageBatchLoad(fileStorage, batch_object);
    // Expect failure (keys don't exist on disk)
    EXPECT_FALSE(result);

    // Metrics should remain 0 - failed operations are not counted
    EXPECT_EQ(ssd_metric.ssd_read_ops.value(), 0);
    EXPECT_EQ(ssd_metric.ssd_read_bytes.value(), 0);

    auto buckets = ssd_metric.ssd_read_latency_us.get_bucket_counts();
    int64_t total_observations = 0;
    for (auto& b : buckets) {
        total_observations += b->value();
    }
    EXPECT_EQ(total_observations, 0);

    LOG(INFO) << "SSD metrics after failed BatchLoad: ops="
              << ssd_metric.ssd_read_ops.value()
              << ", bytes=" << ssd_metric.ssd_read_bytes.value();
}

TEST_F(FileStorageTest, NullSsdMetricDoesNotCrash) {
    // FileStorage with nullptr SsdMetric should work fine (no metrics recorded)
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;

    // nullptr SsdMetric (default)
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");

    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());

    auto allocate_res = FileStorageAllocateBatch(fileStorage, keys, sizes);
    ASSERT_TRUE(allocate_res);

    auto load_result =
        FileStorageBatchLoad(fileStorage, allocate_res.value()->slices);
    ASSERT_TRUE(load_result);
    // No crash = success. No metrics pointer, so nothing to verify.
}

// Issue #3709: a RemoveAll-style physical wipe of the SSD files while master
// still tracks the key's LOCAL_DISK replica leaves a dangling entry. A Put
// for the same key must not report success while storing nothing: the client
// evicts the dangling replica and retries PutStart, so the new data is
// actually written.

TEST_F(FileStorageTest, PutAfterPhysicalWipeHealsDanglingLocalDiskReplica) {
    // The per-key file backend reports the wiped file as OBJECT_NOT_FOUND /
    // FILE_OPEN_FAIL, the original proof set.
    RunPutHealWipeScenario(data_path, StorageBackendType::kFilePerKey,
                           "_per_key");
}

TEST_F(FileStorageTest,
       PutAfterPhysicalWipeHealsDanglingLocalDiskReplicaOnBucket) {
    // The bucket backend's BatchLoad reports a missing key as INVALID_KEY
    // (fanganpai's production trace in #3709); the heal must still fire.
    RunPutHealWipeScenario(data_path, StorageBackendType::kBucket, "_bucket");
}

TEST_F(FileStorageTest,
       BatchPutAfterPhysicalWipeHealsDanglingLocalDiskReplica) {
    // The BatchPut twin of the scenario above: OBJECT_ALREADY_EXISTS results
    // on the batch path must not be converted to success for keys whose
    // backing file is gone. Two wiped keys and one fresh key go through one
    // batch; every key must really store.
    RunBatchPutHealWipeScenario(data_path);
}

TEST_F(FileStorageTest, LocalStreamingRestoreExceedsStagingArenaCapacity) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.local_buffer_size = 8 * 1024 * 1024;
    config.streaming_read.enabled = true;
    config.use_uring =
        std::getenv("MOONCAKE_SSD_TEST_URING") &&
        std::string(std::getenv("MOONCAKE_SSD_TEST_URING")) == "1";
    FileStorage storage(config, nullptr, "local");
    std::string value(20 * 1024 * 1024, 's');
    for (size_t i = 0; i < value.size(); ++i) value[i] = char(i % 251);
    PrepareStreamingObject(storage, "stream-key", value);
    ASSERT_TRUE(storage.SupportsLocalStreamingRead());
    std::vector<char> first(12 * 1024 * 1024), second(8 * 1024 * 1024);
    ASSERT_TRUE(storage.ReadLocalStreaming(
        {{"stream-key",
          value.size(),
          0,
          {{first.data(), first.size()}, {second.data(), second.size()}}}}));
    EXPECT_EQ(std::memcmp(first.data(), value.data(), first.size()), 0);
    EXPECT_EQ(
        std::memcmp(second.data(), value.data() + first.size(), second.size()),
        0);
}

namespace {
class OwnerCopyTarget final : public ssd_offload::SourceDrainedWriteTarget {
   public:
    explicit OwnerCopyTarget(std::vector<Slice> views)
        : views(std::move(views)) {}
    ssd_offload::Error WriteAndDrain(
        const ssd_offload::ReadChunk& chunk,
        ssd_offload::SlotBuffer slot) noexcept override {
        ++writes;
        if (entered && !signalled.exchange(true)) entered->set_value();
        if (release.valid()) release.wait();
        if (fail) return ssd_offload::Error::kTransferError;
        for (const auto& fragment : chunk.fragments) {
            const auto& d = fragment.destination;
            if (!d.region_id || d.region_id > views.size() || d.generation != 1)
                return ssd_offload::Error::kInvalidArgument;
            const auto& view = views[d.region_id - 1];
            if (d.offset > view.size || d.length > view.size - d.offset ||
                fragment.buffer_offset > slot.capacity ||
                d.length > slot.capacity - fragment.buffer_offset)
                return ssd_offload::Error::kInvalidArgument;
            std::memcpy(static_cast<char*>(view.ptr) + d.offset,
                        slot.data + fragment.buffer_offset, d.length);
        }
        return ssd_offload::Error::kOk;
    }
    bool DestinationsDrained() const noexcept override { return drained; }
    std::vector<Slice> views;
    std::atomic<size_t> writes{0};
    bool fail{false}, drained{true};
    std::promise<void>* entered{nullptr};
    std::shared_future<void> release;
    std::atomic<bool> signalled{false};
};
class OwnerTestRegistration final
    : public ssd_offload::DestinationRegistrationTransport {
   public:
    ssd_offload::TransportRegistrationResult RegisterWritable(
        const ssd_offload::DestinationBinding&,
        const std::string&) noexcept override {
        return {ssd_offload::DestinationError::kOk, true, true};
    }
    ssd_offload::DestinationError Unregister(uint64_t) noexcept override {
        return ssd_offload::DestinationError::kOk;
    }
};
}  // namespace

TEST_F(FileStorageTest, OwnerStreamingRemainsIndependentOptIn) {
    auto config = OwnerConfig(data_path);
    config.streaming_read.push_enabled = false;
    config.streaming_read.enabled = true;
    FileStorage local(config, nullptr, "local");
    EXPECT_FALSE(local.SupportsOwnerStreamingRead());
    auto target = std::make_shared<OwnerCopyTarget>(std::vector<Slice>{});
    EXPECT_EQ(local
                  .ReadOwnerStreaming(OwnerRequest("key", 0, 0), target,
                                      std::chrono::steady_clock::now())
                  .error(),
              ErrorCode::NOT_SUPPORTED);
    config.streaming_read.enabled = false;
    config.streaming_read.push_enabled = true;
    FileStorage owner_storage(config, nullptr, "local");
    EXPECT_TRUE(owner_storage.SupportsOwnerStreamingRead());
    EXPECT_FALSE(owner_storage.SupportsLocalStreamingRead());
}

TEST_F(FileStorageTest, OwnerStreamingReadsScopedBatchRangesBeyondArena) {
    FileStorage storage(OwnerConfig(data_path), nullptr, "local");
    std::string value(3 * 1024 * 1024 + 123, 's');
    for (size_t i = 0; i < value.size(); ++i) value[i] = char(i % 251);
    PrepareStreamingObject(
        storage, TenantId("owner-tenant").MakeScopedKey("key"), value);
    std::vector<char> first(value.size() + 64, 'z');
    std::array<char, 128> second;
    second.fill('z');
    auto request = OwnerRequest("key", value.size(), value.size());
    auto ranged = OwnerRequest("key", value.size(), 100, 70001).objects[0];
    ranged.ranges[0].destinations[0].offset = value.size();
    request.objects.push_back(ranged);
    auto target = std::make_shared<OwnerCopyTarget>(std::vector<Slice>{
        {first.data() + 32, value.size()}, {second.data() + 14, 100}});
    auto result = storage.ReadOwnerStreaming(
        request, target,
        std::chrono::steady_clock::now() + std::chrono::seconds(5));
    ASSERT_TRUE(result) << result.error();
    EXPECT_EQ(ssd_offload::ValidateSsdReadResultV1(request, *result),
              ssd_offload::DestinationError::kOk);
    ASSERT_EQ(result->objects.size(), 2u);
    EXPECT_TRUE(result->dma_drained);
    for (const auto& object : result->objects)
        EXPECT_EQ(object.outcome, ssd_offload::AttemptOutcome::kSucceeded);
    EXPECT_EQ(std::memcmp(first.data() + 32, value.data(), value.size()), 0);
    EXPECT_EQ(std::memcmp(second.data() + 14, value.data() + 70001, 100), 0);
    EXPECT_TRUE(std::all_of(first.begin(), first.begin() + 32,
                            [](char c) { return c == 'z'; }));
    EXPECT_TRUE(std::all_of(first.end() - 32, first.end(),
                            [](char c) { return c == 'z'; }));
    EXPECT_TRUE(std::all_of(second.begin(), second.begin() + 14,
                            [](char c) { return c == 'z'; }));
    EXPECT_TRUE(std::all_of(second.end() - 14, second.end(),
                            [](char c) { return c == 'z'; }));
    EXPECT_GT(target->writes.load(), 40u);
    EXPECT_EQ(ActiveStreamingSources(storage), 0u);
    EXPECT_TRUE(
        ReserveStreamingArena(storage, storage.config_.local_buffer_size));
}

TEST_F(FileStorageTest, OwnerStreamingPreflightsAllSourcesAndChecksum) {
    FileStorage storage(OwnerConfig(data_path), nullptr, "local");
    std::string value(12000, 's');
    PrepareStreamingObject(
        storage, TenantId("owner-tenant").MakeScopedKey("key"), value);
    std::array<char, 16> bytes;
    bytes.fill('z');
    auto target = std::make_shared<OwnerCopyTarget>(
        std::vector<Slice>{{bytes.data(), 8}, {bytes.data() + 8, 8}});
    auto request = OwnerRequest("key", value.size(), 8);
    auto missing = OwnerRequest("missing", value.size(), 8).objects[0];
    missing.ranges[0].destinations[0].offset = 8;
    request.objects.push_back(missing);
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    auto result = storage.ReadOwnerStreaming(request, target, deadline);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INVALID_KEY);
    request.objects.pop_back();
    request.objects[0].expected_object_checksum = 123;
    result = storage.ReadOwnerStreaming(request, target, deadline);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::NOT_SUPPORTED);
    EXPECT_EQ(target->writes.load(), 0u);
    EXPECT_EQ(std::string(bytes.data(), bytes.size()), std::string(16, 'z'));
    EXPECT_EQ(ActiveStreamingSources(storage), 0u);
}

TEST_F(FileStorageTest,
       OwnerStreamingDeadlineDrainsAcceptedWriteWithoutRefill) {
    FileStorage storage(OwnerConfig(data_path), nullptr, "local");
    std::string value(3 * 64 * 1024, 's');
    PrepareStreamingObject(
        storage, TenantId("owner-tenant").MakeScopedKey("key"), value);
    std::vector<char> bytes(value.size(), 'z');
    auto request = OwnerRequest("key", value.size(), value.size());
    auto target = std::make_shared<OwnerCopyTarget>(
        std::vector<Slice>{{bytes.data(), bytes.size()}});
    std::promise<void> entered, release;
    auto entry = entered.get_future();
    target->entered = &entered;
    target->release = release.get_future().share();
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(150);
    auto work = std::async(std::launch::async, [&] {
        return storage.ReadOwnerStreaming(request, target, deadline);
    });
    const auto ready = entry.wait_for(std::chrono::seconds(2));
    EXPECT_EQ(ready, std::future_status::ready);
    EXPECT_EQ(work.wait_for(std::chrono::milliseconds(200)),
              std::future_status::timeout);
    EXPECT_EQ(ActiveStreamingSources(storage), 1u);
    EXPECT_FALSE(
        ReserveStreamingArena(storage, storage.config_.local_buffer_size));
    release.set_value();
    auto result = work.get();
    ASSERT_TRUE(result) << result.error();
    EXPECT_TRUE(result->dma_drained);
    ASSERT_EQ(result->objects.size(), 1u);
    EXPECT_EQ(result->objects[0].outcome,
              ssd_offload::AttemptOutcome::kCancelled);
    EXPECT_EQ(result->objects[0].valid_bytes, 64 * 1024u);
    EXPECT_EQ(target->writes.load(), 1u);
    EXPECT_EQ(ssd_offload::ValidateSsdReadResultV1(request, *result),
              ssd_offload::DestinationError::kOk);
    EXPECT_EQ(std::memcmp(bytes.data(), value.data(), 64 * 1024), 0);
    EXPECT_TRUE(std::all_of(bytes.begin() + 64 * 1024, bytes.end(),
                            [](char c) { return c == 'z'; }));
    EXPECT_EQ(ActiveStreamingSources(storage), 0u);
    EXPECT_TRUE(
        ReserveStreamingArena(storage, storage.config_.local_buffer_size));
}

TEST_F(FileStorageTest,
       OwnerStreamingNeverConvertsSourceDrainToDestinationDrain) {
    FileStorage storage(OwnerConfig(data_path), nullptr, "local");
    std::string value(12000, 's');
    PrepareStreamingObject(
        storage, TenantId("owner-tenant").MakeScopedKey("key"), value);
    std::array<char, 16> bytes{};
    auto request = OwnerRequest("key", value.size(), bytes.size());
    auto target = std::make_shared<OwnerCopyTarget>(
        std::vector<Slice>{{bytes.data(), bytes.size()}});
    target->fail = true;
    target->drained = false;
    auto result = storage.ReadOwnerStreaming(
        request, target,
        std::chrono::steady_clock::now() + std::chrono::seconds(5));
    ASSERT_TRUE(result) << result.error();
    EXPECT_FALSE(result->dma_drained);
    EXPECT_EQ(result->objects[0].outcome, ssd_offload::AttemptOutcome::kFailed);
    EXPECT_EQ(result->objects[0].valid_bytes, 0u);
    EXPECT_EQ(ssd_offload::ValidateSsdReadResultV1(request, *result),
              ssd_offload::DestinationError::kNotDrained);
    EXPECT_EQ(target->writes.load(), 1u);
    EXPECT_EQ(ActiveStreamingSources(storage), 0u);
}

TEST_F(FileStorageTest, OwnerStreamingThroughRealControlRpcAndRequesterLease) {
    RunOwnerControlRestore(false);
}
TEST_F(FileStorageTest, OwnerStreamingThroughRealControlRpcAndTcpWriteTarget) {
    RunOwnerControlRestore(true);
}

TEST_F(FileStorageTest, OwnerStreamingThroughOwnedRequesterAndTcpWriteTarget) {
    using namespace ssd_offload;
    auto engine = [] {
        auto result = std::make_shared<TransferEngine>(false);
        if (result->init("P2PHANDSHAKE", "127.0.0.1:0", "127.0.0.1", 0) != 0 ||
            !result->installTransport("tcp", nullptr))
            return std::shared_ptr<TransferEngine>{};
        return result;
    };
    auto source_engine = engine(), destination_engine = engine();
    ASSERT_TRUE(source_engine);
    ASSERT_TRUE(destination_engine);
    if (source_engine->isUsingTent()) GTEST_SKIP() << "Requires classic TE TCP";
    auto storage =
        std::make_shared<FileStorage>(OwnerConfig(data_path), nullptr, "local");
    auto arena_registration = RegisterStreamingArena(storage, source_engine);
    ASSERT_TRUE(arena_registration);
    std::string value(3 * 1024 * 1024 + 123, 's');
    for (size_t i = 0; i < value.size(); ++i) value[i] = char(i % 251);
    PrepareStreamingObject(
        *storage, TenantId("owner-tenant").MakeScopedKey("key"), value);
    const PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
    auto ledger = std::make_shared<SsdOwnerRequestLedger>(owner);
    std::shared_ptr<SsdOwnerReadWorker> worker;
    SsdControlRpcEndpoint owner_endpoint(
        {}, ledger,
        [&worker](const auto& request, const auto& reference, auto operation) {
            EXPECT_TRUE(worker);
            if (worker)
                EXPECT_EQ(worker->Run(request, reference, std::move(operation)),
                          DestinationError::kOk);
        });
    SsdControlRpcServer owner_server(1, 0, "127.0.0.1");
    owner_server.register_handler<&SsdControlRpcEndpoint::Call>(
        &owner_endpoint);
    owner_server.async_start();
    ASSERT_FALSE(owner_server.get_errc());
    const auto data_endpoint = destination_engine->getLocalIpAndPort();
    auto transport = std::make_shared<TransferEngineDestinationRegistration>(
        destination_engine);
    SsdPushPeerInfo owner_peer{owner,
                               "owner-tenant",
                               "owner-legacy:9000",
                               "127.0.0.1",
                               owner_server.port(),
                               source_engine->getLocalIpAndPort(),
                               {1,
                                owner,
                                15,
                                64,
                                64 * 1024 * 1024,
                                65536,
                                64 * 1024 * 1024,
                                65536,
                                {"tcp"}}};
    SsdPushRequester client(requester, "owner-tenant", data_endpoint, transport,
                            {owner_peer}, MakeSsdRequesterControlCall());
    SsdControlRpcEndpoint requester_endpoint(client.grants(), {});
    SsdControlRpcServer requester_server(1, 0, "127.0.0.1");
    requester_server.register_handler<&SsdControlRpcEndpoint::Call>(
        &requester_endpoint);
    requester_server.async_start();
    ASSERT_FALSE(requester_server.get_errc());
    SsdOwnerRequesterPeer requester_peer{
        requester, "owner-tenant", data_endpoint,
        [port = requester_server.port()](const auto& reference,
                                         const auto& request, auto timeout) {
            auto resolved = CallSsdControlRpc(
                "127.0.0.1", port,
                {SsdControlCommand::kResolve, reference, request}, timeout);
            return resolved.error == DestinationError::kOk && resolved.reply
                       ? SsdGrantResolution{resolved.reply->error,
                                            std::move(
                                                resolved.reply->destinations)}
                       : SsdGrantResolution{DestinationError::kTransportError,
                                            {}};
        }};
    worker = std::make_shared<SsdOwnerReadWorker>(
        owner, std::vector<SsdOwnerRequesterPeer>{requester_peer},
        [source_engine](auto views) {
            return PrepareSsdTcpWriteTarget(source_engine, std::move(views))
                .target;
        },
        MakeFileStorageOwnerReader(storage));
    constexpr size_t offset = 12345, length = 150000;
    auto started = client.Start("owner-legacy:9000",
                                {{"key", value.size(), 0, value.size()},
                                 {"key", value.size(), offset, length}},
                                std::chrono::seconds(5));
    ASSERT_EQ(started.error, DestinationError::kOk);
    ASSERT_TRUE(started.may_have_submitted);
    ASSERT_TRUE(started.read);
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!started.read->data(0) &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_EQ(started.read->Poll(std::chrono::seconds(1)),
                  DestinationError::kOk);
        if (!started.read->data(0))
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    auto full = started.read->data(0), range = started.read->data(1);
    ASSERT_TRUE(full);
    ASSERT_TRUE(range);
    EXPECT_EQ(full->size(), value.size());
    EXPECT_EQ(std::memcmp(full->data(), value.data(), value.size()), 0);
    EXPECT_EQ(range->size(), length);
    EXPECT_EQ(std::memcmp(range->data(), value.data() + offset, length), 0);
    full.reset();
    range.reset();
    EXPECT_EQ(started.read->Release(std::chrono::seconds(1)),
              DestinationError::kOk);
    EXPECT_EQ(client.stats().owned_bytes, 0u);
    EXPECT_EQ(client.stats().pinned_regions, 0u);
    EXPECT_EQ(ledger->stats().records, 0u);
    owner_server.stop();
    requester_server.stop();
    EXPECT_EQ(client.Close(), DestinationError::kOk);
}

void FileStorageTest::RunOwnerControlRestore(bool tcp) {
    using namespace ssd_offload;
    std::weak_ptr<FileStorage> lifetime;
    {
        auto create_engine = [] {
            auto engine = std::make_shared<TransferEngine>(false);
            if (engine->init("P2PHANDSHAKE", "127.0.0.1:0", "127.0.0.1", 0) !=
                    0 ||
                !engine->installTransport("tcp", nullptr))
                return std::shared_ptr<TransferEngine>{};
            return engine;
        };
        auto source_engine = tcp ? create_engine() : nullptr;
        auto destination_engine = tcp ? create_engine() : nullptr;
        if (tcp) {
            ASSERT_TRUE(source_engine);
            ASSERT_TRUE(destination_engine);
            if (source_engine->isUsingTent())
                GTEST_SKIP() << "Requires classic TE TCP";
        }
        auto storage = std::make_shared<FileStorage>(OwnerConfig(data_path),
                                                     nullptr, "local");
        lifetime = storage;
        auto arena_registration =
            tcp ? RegisterStreamingArena(storage, source_engine) : nullptr;
        if (tcp) ASSERT_TRUE(arena_registration);
        std::string value(3 * 1024 * 1024 + 123, 's');
        for (size_t i = 0; i < value.size(); ++i) value[i] = char(i % 251);
        PrepareStreamingObject(
            *storage, TenantId("owner-tenant").MakeScopedKey("key"), value);
        const PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
        auto buffer =
            std::make_shared<std::vector<char>>(value.size() + 64, 'z');
        const auto base = reinterpret_cast<uint64_t>(buffer->data());
        std::shared_ptr<DestinationRegistrationTransport> transport;
        if (tcp)
            transport = std::make_shared<TransferEngineDestinationRegistration>(
                destination_engine);
        else
            transport = std::make_shared<OwnerTestRegistration>();
        const std::string data_endpoint =
            tcp ? destination_engine->getLocalIpAndPort() : "requester-te";
        auto registration = std::make_shared<DestinationRegistration>(
            requester, data_endpoint, transport);
        auto region = registration->Register(
            {base + 32, value.size(), base, buffer->size()}, "cpu:0", buffer);
        ASSERT_EQ(region.error, DestinationError::kOk);
        auto grants =
            std::make_shared<SsdRequesterGrants>(requester, registration);
        auto request = OwnerRequest("key", value.size(), value.size());
        request.objects[0].ranges[0].destinations[0].region = region.region;
        auto prepared = grants->Prepare(request);
        ASSERT_EQ(prepared.error, DestinationError::kOk);
        SsdControlRpcEndpoint requester_endpoint(grants, {});
        SsdControlRpcServer requester_server(1, 0, "127.0.0.1");
        requester_server.register_handler<&SsdControlRpcEndpoint::Call>(
            &requester_endpoint);
        requester_server.async_start();
        ASSERT_FALSE(requester_server.get_errc());
        SsdOwnerRequesterPeer peer{
            requester, "owner-tenant", data_endpoint,
            [port = requester_server.port()](const auto& ref, const auto& req,
                                             auto timeout) {
                auto call = CallSsdControlRpc(
                    "127.0.0.1", port, {SsdControlCommand::kResolve, ref, req},
                    timeout);
                if (call.transport_error ||
                    call.error != DestinationError::kOk || !call.reply)
                    return SsdGrantResolution{DestinationError::kTransportError,
                                              {}};
                return SsdGrantResolution{call.reply->error,
                                          std::move(call.reply->destinations)};
            }};
        std::atomic<size_t> readers{0};
        auto reader = MakeFileStorageOwnerReader(storage);
        auto worker = std::make_shared<SsdOwnerReadWorker>(
            owner, std::vector<SsdOwnerRequesterPeer>{std::move(peer)},
            [tcp, source_engine](
                auto views) -> std::shared_ptr<SourceDrainedWriteTarget> {
                if (tcp) {
                    auto prepared = PrepareSsdTcpWriteTarget(source_engine,
                                                             std::move(views));
                    EXPECT_EQ(prepared.error, DestinationError::kOk);
                    return prepared.target;
                }
                std::vector<Slice> slices;
                for (const auto& view : views)
                    slices.push_back({reinterpret_cast<void*>(view.address),
                                      size_t(view.length)});
                return std::make_shared<OwnerCopyTarget>(std::move(slices));
            },
            [reader = std::move(reader), &readers](const auto& req, auto target,
                                                   auto deadline) {
                ++readers;
                return reader(req, std::move(target), deadline);
            });
        storage.reset();  // Adapter retains the real backend and staging arena.
        EXPECT_FALSE(lifetime.expired());
        auto ledger = std::make_shared<SsdOwnerRequestLedger>(owner);
        SsdControlRpcEndpoint owner_endpoint(
            {}, ledger, [worker](const auto& req, const auto& ref, auto op) {
                EXPECT_EQ(worker->Run(req, ref, std::move(op)),
                          DestinationError::kOk);
            });
        SsdControlRpcServer owner_server(1, 0, "127.0.0.1");
        owner_server.register_handler<&SsdControlRpcEndpoint::Call>(
            &owner_endpoint);
        owner_server.async_start();
        ASSERT_FALSE(owner_server.get_errc());
        auto call = [&](SsdControlCommand command, bool with_request = false) {
            return CallSsdControlRpc(
                "127.0.0.1", owner_server.port(),
                {command, prepared.reference,
                 with_request ? std::optional(request) : std::nullopt},
                std::chrono::seconds(2));
        };
        auto submitted = call(SsdControlCommand::kSubmit, true);
        ASSERT_FALSE(submitted.transport_error);
        ASSERT_TRUE(submitted.reply);
        EXPECT_EQ(submitted.reply->error, DestinationError::kOk);
        auto duplicate = call(SsdControlCommand::kSubmit, true);
        ASSERT_TRUE(duplicate.reply);
        EXPECT_EQ(duplicate.reply->error, DestinationError::kDuplicate);
        std::optional<SsdReadResultV1> terminal;
        const auto until =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < until) {
            auto query = call(SsdControlCommand::kQuery);
            ASSERT_FALSE(query.transport_error);
            ASSERT_TRUE(query.reply);
            ASSERT_TRUE(query.reply->snapshot);
            if (query.reply->snapshot->result) {
                terminal = query.reply->snapshot->result;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        ASSERT_TRUE(terminal);
        EXPECT_EQ(terminal->objects[0].outcome, AttemptOutcome::kSucceeded);
        EXPECT_EQ(terminal->objects[0].valid_bytes, value.size());
        EXPECT_EQ(readers.load(), 1u);
        EXPECT_EQ(std::memcmp(buffer->data() + 32, value.data(), value.size()),
                  0);
        EXPECT_TRUE(std::all_of(buffer->begin(), buffer->begin() + 32,
                                [](char c) { return c == 'z'; }));
        EXPECT_TRUE(std::all_of(buffer->end() - 32, buffer->end(),
                                [](char c) { return c == 'z'; }));
        EXPECT_EQ(grants->Complete(prepared.reference, *terminal),
                  DestinationError::kOk);
        EXPECT_TRUE(grants->Inspect(prepared.reference).publishable);
        auto forgotten = call(SsdControlCommand::kForget);
        ASSERT_TRUE(forgotten.reply);
        EXPECT_EQ(forgotten.reply->error, DestinationError::kOk);
        EXPECT_EQ(grants->Forget(owner, request.attempt.request_id),
                  DestinationError::kOk);
        EXPECT_EQ(registration->Revoke(region.region), DestinationError::kOk);
        owner_server.stop();
        requester_server.stop();
    }
    EXPECT_TRUE(lifetime.expired());
}

TEST_F(FileStorageTest, LocalStreamingRangeAndInvalidRange) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.local_buffer_size = 8 * 1024 * 1024;
    config.streaming_read.enabled = true;
    config.use_uring =
        std::getenv("MOONCAKE_SSD_TEST_URING") &&
        std::string(std::getenv("MOONCAKE_SSD_TEST_URING")) == "1";
    FileStorage storage(config, nullptr, "local");
    std::string value(12000, 'x');
    value.replace(300, 5, "hello");
    PrepareStreamingObject(storage, "range-key", value);
    std::array<char, 5> destination{};
    ASSERT_TRUE(storage.ReadLocalStreaming(
        {{"range-key",
          value.size(),
          300,
          {{destination.data(), destination.size()}}}}));
    EXPECT_EQ(std::string(destination.data(), destination.size()), "hello");
    destination.fill('z');
    auto invalid = storage.ReadLocalStreaming(
        {{"range-key",
          value.size(),
          value.size(),
          {{destination.data(), destination.size()}}}});
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(std::string(destination.data(), destination.size()), "zzzzz");
}

TEST_F(FileStorageTest, LocalStreamingRemainsOptIn) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.local_buffer_size = 8 * 1024 * 1024;
    FileStorage storage(config, nullptr, "local");
    EXPECT_FALSE(storage.SupportsLocalStreamingRead());
    auto result = storage.ReadLocalStreaming({});
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::NOT_SUPPORTED);
}

TEST_F(FileStorageTest, LocalStreamingPreflightsBatchAndRejectsOverlap) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.local_buffer_size = 8 * 1024 * 1024;
    config.streaming_read.enabled = true;
    FileStorage storage(config, nullptr, "local");
    std::string value(12000, 's');
    PrepareStreamingObject(storage, "valid-key", value);
    std::array<char, 16> destination;
    destination.fill('z');
    auto bad_key = storage.ReadLocalStreaming(
        {{"valid-key", value.size(), 0, {{destination.data(), 8}}},
         {"missing-key", value.size(), 0, {{destination.data() + 8, 8}}}});
    ASSERT_FALSE(bad_key);
    EXPECT_EQ(bad_key.error(), ErrorCode::INVALID_KEY);
    EXPECT_EQ(std::string(destination.data(), 16), std::string(16, 'z'));
    auto overlap = storage.ReadLocalStreaming(
        {{"valid-key", value.size(), 0, {{destination.data(), 8}}},
         {"valid-key", value.size(), 8, {{destination.data() + 4, 8}}}});
    ASSERT_FALSE(overlap);
    EXPECT_EQ(overlap.error(), ErrorCode::INVALID_PARAMS);
    auto null = storage.ReadLocalStreaming(
        {{"valid-key", value.size(), 0, {{nullptr, 1}}}});
    ASSERT_FALSE(null);
    EXPECT_EQ(null.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_TRUE(storage.ReadLocalStreaming({}));
}

TEST_F(FileStorageTest, LocalStreamingBoundsOpenSourcesBeforeWriting) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.local_buffer_size = 8 * 1024 * 1024;
    config.streaming_read.enabled = true;
    config.streaming_read.max_outstanding = 8;
    FileStorage storage(config, nullptr, "local");
    std::string value(12000, 's');
    PrepareStreamingObject(storage, "bounded-key", value);
    std::array<char, 9> destination;
    destination.fill('z');
    std::vector<FileStorage::LocalReadRequest> requests;
    for (size_t i = 0; i < destination.size(); ++i)
        requests.push_back(
            {"bounded-key", value.size(), i, {{destination.data() + i, 1}}});
    auto rejected = storage.ReadLocalStreaming(requests);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(std::string(destination.data(), 9), std::string(9, 'z'));
    requests.pop_back();
    EXPECT_TRUE(storage.ReadLocalStreaming(requests));
}

TEST_F(FileStorageTest,
       LocalStreamingReleasesPartialArenaReservationOnFailure) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.local_buffer_size = 8 * 1024 * 1024;
    config.streaming_read.enabled = true;
    FileStorage storage(config, nullptr, "local");
    std::string value(6 * 1024 * 1024, 's');
    PrepareStreamingObject(storage, "arena-key", value);
    std::vector<char> destination(value.size(), 'z');
    auto held = ReserveStreamingArena(storage, 6 * 1024 * 1024);
    ASSERT_TRUE(held);
    const std::vector<FileStorage::LocalReadRequest> requests{
        {"arena-key",
         value.size(),
         0,
         {{destination.data(), destination.size()}}}};
    auto failed = storage.ReadLocalStreaming(requests);
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_TRUE(std::all_of(destination.begin(), destination.end(),
                            [](char c) { return c == 'z'; }));
    held.reset();
    // Any leaked partial reservation would prevent allocating the full arena.
    auto whole_arena = ReserveStreamingArena(storage, config.local_buffer_size);
    ASSERT_TRUE(whole_arena);
    whole_arena.reset();
    ASSERT_TRUE(storage.ReadLocalStreaming(requests));
    EXPECT_EQ(std::memcmp(destination.data(), value.data(), value.size()), 0);
}

TEST_F(FileStorageTest, LocalStreamingSharesSourceCreditsAcrossCallers) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.local_buffer_size = 8 * 1024 * 1024;
    config.streaming_read.enabled = true;
    config.streaming_read.slots = 1;
    config.streaming_read.read_workers = 1;
    config.streaming_read.max_outstanding = 2;
    FileStorage storage(config, nullptr, "local");
    std::string value(12000, 's');
    PrepareStreamingObject(storage, "concurrent-key", value);
    std::array<char, 3> destination{};
    std::promise<void> started, release;
    auto released = release.get_future().share();
    ASSERT_TRUE(SubmitStreamingReadTask(storage, [&] {
        started.set_value();
        released.wait();
    }));
    started.get_future().wait();
    auto first = std::async(std::launch::async, [&] {
        return storage.ReadLocalStreaming(
            {{"concurrent-key", value.size(), 0, {{destination.data(), 1}}}});
    });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!ActiveStreamingSources(storage) &&
           std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    EXPECT_EQ(ActiveStreamingSources(storage), 1u);
    const std::vector<FileStorage::LocalReadRequest> second{
        {"concurrent-key", value.size(), 1, {{destination.data() + 1, 1}}},
        {"concurrent-key", value.size(), 2, {{destination.data() + 2, 1}}}};
    auto concurrent = std::async(
        std::launch::async, [&] { return storage.ReadLocalStreaming(second); });
    EXPECT_EQ(concurrent.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    // Unblock accepted I/O even if a broken admission path failed assertions.
    release.set_value();
    auto rejected = concurrent.get();
    EXPECT_FALSE(rejected);
    if (!rejected) EXPECT_EQ(rejected.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_TRUE(first.get());
    EXPECT_TRUE(storage.ReadLocalStreaming(second));
    EXPECT_EQ(std::string(destination.data(), 3), "sss");
}

}  // namespace mooncake

namespace mooncake {
TEST_F(FileStorageTest,
       OwnerStreamingUsesNativeUringInsteadOfSynchronousReadAt) {
    auto config = OwnerConfig(data_path);
    config.streaming_read.use_uring = true;
    FileStorage storage(config, nullptr, "local");
    InstallNativeOnlyBackend(storage, config);
    std::string value(150000, 'u');
    PrepareStreamingObject(
        storage, TenantId("owner-tenant").MakeScopedKey("native-key"), value);
    std::vector<char> destination(value.size(), '?');
    auto target = std::make_shared<OwnerCopyTarget>(
        std::vector<Slice>{{destination.data(), destination.size()}});
    auto result = storage.ReadOwnerStreaming(
        OwnerRequest("native-key", value.size(), value.size()), target,
        std::chrono::steady_clock::now() + std::chrono::seconds(2));
#ifdef USE_URING
    ASSERT_TRUE(result) << (result ? "success" : toString(result.error()));
    EXPECT_EQ(result->objects[0].outcome,
              ssd_offload::AttemptOutcome::kSucceeded);
    EXPECT_EQ(destination, std::vector<char>(value.begin(), value.end()));
#else
    EXPECT_FALSE(result);
#endif
}

TEST_F(FileStorageTest, OwnerStreamingNativeBatchSwitchesSourcesAndRanges) {
    auto config = OwnerConfig(data_path);
    config.streaming_read.use_uring = true;
    config.streaming_read.slots = 2;
    FileStorage storage(config, nullptr, "local");
    InstallNativeOnlyBackend(storage, config);
    std::string first(150000, 'a'), second(160007, 'b');
    for (size_t i = 0; i < first.size(); ++i)
        first[i] = char((i * 37 + 5) % 251);
    for (size_t i = 0; i < second.size(); ++i)
        second[i] = char((i * 53 + 11) % 251);
    PrepareStreamingBatch(storage,
                          {{TenantId("owner-tenant").MakeScopedKey("first"),
                            {{first.data(), first.size()}}},
                           {TenantId("owner-tenant").MakeScopedKey("second"),
                            {{second.data(), second.size()}}}});
    constexpr size_t guard = 32, offset = 61, length = 70001;
    std::vector<char> destination(guard + first.size() + guard + length + guard,
                                  '?');
    auto request = OwnerRequest("first", first.size(), first.size());
    request.objects[0].ranges[0].destinations[0].offset = guard;
    auto next =
        OwnerRequest("second", second.size(), length, offset).objects[0];
    const auto next_destination = guard + first.size() + guard;
    auto target = std::make_shared<OwnerCopyTarget>(
        std::vector<Slice>{{destination.data() + guard, first.size()},
                           {destination.data() + next_destination, length}});
    next.ranges[0].destinations[0].offset = next_destination;
    request.objects.push_back(std::move(next));
    auto result = storage.ReadOwnerStreaming(
        request, target,
        std::chrono::steady_clock::now() + std::chrono::seconds(2));
#ifdef USE_URING
    ASSERT_TRUE(result) << (result ? "success" : toString(result.error()));
    ASSERT_EQ(result->objects.size(), 2u);
    for (const auto& object : result->objects)
        EXPECT_EQ(object.outcome, ssd_offload::AttemptOutcome::kSucceeded);
    EXPECT_EQ(std::string(destination.data() + guard, first.size()), first);
    EXPECT_EQ(std::string(destination.data() + next_destination, length),
              second.substr(offset, length));
    for (size_t i = 0; i < destination.size(); ++i) {
        if (i < guard || (i >= guard + first.size() && i < next_destination) ||
            i >= next_destination + length) {
            EXPECT_EQ(destination[i], '?');
        }
    }
    EXPECT_TRUE(result->dma_drained);
#else
    EXPECT_FALSE(result);
#endif
}
}  // namespace mooncake
