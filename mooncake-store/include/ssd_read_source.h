// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <memory>
#include <optional>
#include <span>
#include <string>

#include "ssd_read_pipeline.h"

namespace mooncake::ssd_offload {

struct ReadSourceInfo {
    uint64_t object_bytes;
    uint64_t value_file_offset;
    size_t alignment;
};

struct ReadResult {
    size_t bytes{0};
    Error error{Error::kOk};
};

class ReadSource {
   public:
    virtual ~ReadSource() = default;
    virtual ReadSourceInfo info() const noexcept = 0;
    // Synchronous, position-independent read. When this returns, no I/O
    // can still write into the supplied buffer, including on failure.
    virtual ReadResult ReadAt(std::span<std::byte> buffer,
                              uint64_t file_offset) noexcept = 0;
    // Borrowed fd, pinned by the source lifetime. Async drivers must retain
    // this source until every submitted read has actually completed.
    virtual std::optional<int> native_read_fd() const noexcept { return {}; }
};

struct ReadSourceResult {
    Error error;
    std::shared_ptr<ReadSource> source;
};

// Buffered POSIX compatibility source. alignment is a planner requirement,
// not a promise that the file was opened with O_DIRECT. The optional token
// pins backend metadata/file lifetime through the last source reference.
ReadSourceResult OpenPosixReadSource(const std::string& path,
                                     ReadSourceInfo info,
                                     std::shared_ptr<void> lifetime = {});

}  // namespace mooncake::ssd_offload
