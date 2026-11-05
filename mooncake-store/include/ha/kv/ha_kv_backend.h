#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "types.h"

namespace mooncake {

struct KvPair {
    std::string key;
    std::string value;
};

enum class KvCompareKind {
    kValueEquals,
    kKeyNotExists,
    kCreateRevisionEquals,
};

struct KvCompare {
    std::string key;
    KvCompareKind kind{KvCompareKind::kValueEquals};
    std::string expected_value;
    // Used only by kCreateRevisionEquals.
    EtcdRevisionId expected_revision{0};
};

struct KvTxn {
    std::vector<KvCompare> compares;
    std::vector<KvPair> puts;
};

class HaKvBackend {
   public:
    virtual ~HaKvBackend() = default;

    virtual ErrorCode Get(std::string_view key, std::string& value) = 0;
    virtual ErrorCode Put(std::string_view key, std::string_view value) = 0;
    virtual ErrorCode Range(std::string_view begin_key,
                            std::string_view end_key, size_t limit,
                            std::vector<KvPair>& kvs) = 0;
    // Read at most limit keys in ascending order from a finite range, without
    // fetching values. A zero limit is invalid. Unsupported backends fail
    // closed rather than falling back to potentially large value reads.
    virtual ErrorCode RangeKeys(std::string_view, std::string_view, size_t,
                                std::vector<std::string>& keys) {
        keys.clear();
        return ErrorCode::INVALID_PARAMS;
    }
    // Delete [begin_key, end_key). Both bounds must be nonempty and finite.
    // Equal bounds are a no-op for backends supporting deletion.
    // Unsupported backends must return INVALID_PARAMS.
    virtual ErrorCode DeleteRange(std::string_view begin_key,
                                  std::string_view end_key) = 0;
    // The count is valid only on success. Never emulate this with an
    // uncounted delete: callers use the actual count to track cleanup progress.
    virtual ErrorCode DeleteRangeCounted(std::string_view, std::string_view,
                                         uint64_t& deleted) {
        deleted = 0;
        return ErrorCode::INVALID_PARAMS;
    }
    virtual bool SupportsTxn() const = 0;
    virtual ErrorCode Txn(const KvTxn& txn) = 0;
};

}  // namespace mooncake
