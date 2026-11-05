#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

#include "types.h"

namespace mooncake {

class HaKvBackend;
class SnapshotObjectStore;
class SnapshotMaintenanceLease;

struct BatchOpLogPruningConfig {
    // Counts stored oplog batch keys, not entries within each batch.
    size_t max_batches_per_request{4096};
    uint64_t batch_interval_ms{20};
};

// Publishes a safe floor after snapshot publication, then removes the physical
// prefix in bounded passes under the maintenance lease. The reader-visible
// floor always precedes deletion and is not a physical-cleanup cursor.
class BatchOpLogPruningCoordinator {
   public:
    BatchOpLogPruningCoordinator(HaKvBackend& backend,
                                 SnapshotObjectStore& object_store,
                                 std::string cluster_id,
                                 std::string snapshot_root,
                                 BatchOpLogPruningConfig config = {});

    ErrorCode Run(const SnapshotMaintenanceLease& lease,
                  const std::function<bool()>& cancelled = {},
                  bool* more_work = nullptr);

    // Continues cleanup using only an already published floor. It does not
    // require a new snapshot or re-publish a floor. A successful pass with
    // more_work=true must be scheduled again even when no new PUTs arrive.
    ErrorCode Resume(const SnapshotMaintenanceLease& lease,
                     const std::function<bool()>& cancelled = {},
                     bool* more_work = nullptr);

   private:
    bool ValidContext(const SnapshotMaintenanceLease& lease) const;
    ErrorCode Drain(uint64_t floor, const SnapshotMaintenanceLease& lease,
                    const std::function<bool()>& cancelled, bool* more_work);

    HaKvBackend& backend_;
    SnapshotObjectStore& object_store_;
    std::string cluster_id_;
    std::string snapshot_root_;
    BatchOpLogPruningConfig config_;
};

}  // namespace mooncake
