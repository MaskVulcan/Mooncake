// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_uring_pipeline.h"
#if defined(__linux__) && defined(USE_URING)
#include <liburing.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <algorithm>
#include <climits>
#include <condition_variable>
#include <limits>
#include <mutex>

namespace mooncake::ssd_offload {
namespace {
bool Same(const SlotToken& a, const SlotToken& b) {
    return a.request_id == b.request_id && a.attempt_id == b.attempt_id &&
           a.slot_index == b.slot_index && a.generation == b.generation &&
           a.chunk_index == b.chunk_index;
}
}  // namespace
struct UringPipelineDriver::Impl {
    enum class Stage { kIdle, kRead, kWrite, kReady };
    struct Record {
        Stage stage{Stage::kIdle};
        IoCompletion event;
        bool cancelled{false};
    };
    Impl(std::shared_ptr<ReadSource> source,
         std::shared_ptr<SourceDrainedWriteTarget> target,
         BoundedExecutor& writes, std::vector<SlotBuffer> buffers)
        : source(std::move(source)),
          target(std::move(target)),
          writes(writes),
          buffers(std::move(buffers)),
          records(this->buffers.size()) {}
    bool Init(int fd) {
        if (io_uring_queue_init(unsigned(std::max(size_t{2}, buffers.size())),
                                &ring, 0) < 0)
            return false;
        initialized = true;
        signal = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (signal < 0 || io_uring_register_eventfd(&ring, signal) < 0)
            return false;
        std::vector<iovec> views;
        views.reserve(buffers.size());
        for (const auto& buffer : buffers)
            views.push_back({buffer.data, buffer.capacity});
        if (io_uring_register_buffers(&ring, views.data(),
                                      unsigned(views.size())) < 0)
            return false;
        buffers_registered = true;
        const int registered = io_uring_register_files(&ring, &fd, 1);
        if (registered < 0) {
            // Some vendor 5.10 kernels leave the fresh fixed-file table in
            // place while reporting EBADF. Bind its only slot explicitly and
            // require a real update acknowledgment before accepting any SQE.
            // Invalid descriptors or a missing table still fail this check.
            if (registered != -EBADF ||
                io_uring_register_files_update(&ring, 0, &fd, 1) != 1)
                return false;
        }
        files_registered = true;
        return true;
    }
    ~Impl() {
        {
            std::lock_guard lock(mutex);
            stopping = true;
            for (auto& record : records) record.cancelled = true;
        }
        // Accepted SQEs, including ones not submitted yet, must produce a CQE.
        // Source buffers and chunk metadata remain pinned through this barrier.
        for (;;) {
            {
                std::lock_guard lock(mutex);
                if (!reads_running && !writes_running) break;
            }
            IoCompletion ignored;
            Wait(ignored, std::chrono::milliseconds(100));
        }
        if (files_registered) io_uring_unregister_files(&ring);
        if (buffers_registered) io_uring_unregister_buffers(&ring);
        if (initialized) io_uring_queue_exit(&ring);
        if (signal >= 0) close(signal);
    }
    Error Rebind(std::shared_ptr<ReadSource> next) noexcept {
        if (!next) return Error::kInvalidArgument;
        const auto fd = next->native_read_fd();
        if (!fd || *fd < 0) return Error::kNotSupported;
        try {
            std::lock_guard lock(mutex);
            if (stopping || reads_running || writes_running ||
                std::any_of(records.begin(), records.end(), [](const auto& r) {
                    return r.stage != Stage::kIdle;
                }))
                return Error::kInvalidState;
            // One acknowledged replacement, with no accepted work remaining.
            // Keep the old source alive if the replacement is not confirmed.
            if (io_uring_register_files_update(&ring, 0, &*fd, 1) != 1) {
                stopping = true;  // Do not submit through an uncertain binding.
                return Error::kIoError;
            }
            source = std::move(next);
            return Error::kOk;
        } catch (...) {
            return Error::kIoError;
        }
    }
    bool Valid(const SlotToken& token, const ReadChunk& chunk,
               SlotBuffer buffer) const {
        return token.request_id && token.attempt_id && token.generation &&
               token.slot_index < buffers.size() && buffer.data &&
               chunk.read_bytes &&
               buffer.data == buffers[token.slot_index].data &&
               buffer.capacity == buffers[token.slot_index].capacity &&
               chunk.read_bytes <= buffer.capacity &&
               chunk.read_bytes <= UINT_MAX &&
               chunk.file_offset <=
                   uint64_t(std::numeric_limits<off_t>::max()) &&
               chunk.read_bytes <= uint64_t(std::numeric_limits<off_t>::max()) -
                                       chunk.file_offset;
    }
    void Flush() noexcept {
        // SQPOLL is disabled: accepted reads are batched until the scheduler
        // next polls. Submission errors are not completion/drain evidence.
        while (initialized && io_uring_sq_ready(&ring)) {
            const int submitted = io_uring_submit(&ring);
            if (submitted <= 0) break;
        }
    }
    bool Read(const SlotToken& token, const ReadChunk& chunk,
              SlotBuffer buffer) noexcept {
        if (!Valid(token, chunk, buffer)) return false;
        std::lock_guard lock(mutex);
        auto& record = records[token.slot_index];
        if (stopping || record.stage != Stage::kIdle) return false;
        auto* sqe = io_uring_get_sqe(&ring);
        if (!sqe) return false;  // No SQE published for this slot.
        io_uring_prep_read_fixed(sqe, 0, buffer.data,
                                 unsigned(chunk.read_bytes), chunk.file_offset,
                                 int(token.slot_index));
        sqe->flags |= IOSQE_FIXED_FILE;
        io_uring_sqe_set_data64(sqe, token.slot_index + 1);
        record.stage = Stage::kRead;
        record.cancelled = false;
        record.event = {token, SlotStage::kReading, 0, Error::kOk};
        ++reads_running;
        return true;
    }
    bool Write(const SlotToken& token, const ReadChunk& chunk,
               SlotBuffer buffer) noexcept {
        if (!Valid(token, chunk, buffer)) return false;
        {
            std::lock_guard lock(mutex);
            auto& record = records[token.slot_index];
            if (stopping || record.stage != Stage::kIdle) return false;
            record.stage = Stage::kWrite;
            record.cancelled = false;
            record.event = {token, SlotStage::kWriting, 0, Error::kOk};
            ++writes_running;
        }
        bool accepted = false;
        try {
            accepted =
                writes.TrySubmit([this, token, buffer, chunk_ptr = &chunk] {
                    bool cancelled;
                    {
                        std::lock_guard lock(mutex);
                        cancelled = records[token.slot_index].cancelled;
                    }
                    const auto result =
                        cancelled ? Error::kCancelled
                                  : target->WriteAndDrain(*chunk_ptr, buffer);
                    {
                        std::lock_guard lock(mutex);
                        auto& record = records[token.slot_index];
                        record.event.result = result;
                        record.event.bytes =
                            result == Error::kOk ? chunk_ptr->payload_bytes : 0;
                        record.stage = Stage::kReady;
                        uint64_t one = 1;
                        // Signal is part of the callback's physical quiescence.
                        // Keep the lock/count until its last access to this
                        // ring.
                        while (::write(signal, &one, sizeof(one)) < 0 &&
                               errno == EINTR) {
                        }
                        --writes_running;
                    }
                });
        } catch (...) {
        }
        if (!accepted) {
            std::lock_guard lock(mutex);
            records[token.slot_index].stage = Stage::kIdle;
            --writes_running;
        }
        return accepted;
    }
    void Cancel(const SlotToken& token, SlotStage stage) noexcept {
        std::lock_guard lock(mutex);
        if (token.slot_index >= records.size()) return;
        auto& record = records[token.slot_index];
        if (Same(token, record.event.token) && record.event.stage == stage)
            record.cancelled = true;
        // Cancellation stops refill in ReadPipeline. Already accepted READs
        // are still flushed/drained rather than pretending cancellation ended
        // I/O.
    }
    void Harvest() {
        io_uring_cqe* cqe = nullptr;
        while (initialized && io_uring_peek_cqe(&ring, &cqe) == 0) {
            const auto id = io_uring_cqe_get_data64(cqe);
            {
                std::lock_guard lock(mutex);
                if (id && id <= records.size()) {
                    auto& record = records[size_t(id - 1)];
                    if (record.stage == Stage::kRead) {
                        record.event.bytes =
                            cqe->res >= 0 ? size_t(cqe->res) : 0;
                        record.event.result = record.cancelled
                                                  ? Error::kCancelled
                                              : cqe->res < 0 ? Error::kIoError
                                                             : Error::kOk;
                        record.stage = Stage::kReady;
                        --reads_running;
                    }
                }
            }
            io_uring_cqe_seen(&ring, cqe);
        }
    }
    bool Ready(IoCompletion& event) {
        std::lock_guard lock(mutex);
        for (auto& record : records) {
            if (record.stage != Stage::kReady) continue;
            event = record.event;
            record.stage = Stage::kIdle;
            return true;
        }
        return false;
    }
    bool Wait(IoCompletion& event, std::chrono::milliseconds timeout) {
        const auto until = std::chrono::steady_clock::now() +
                           std::max(timeout, std::chrono::milliseconds::zero());
        Flush();
        for (;;) {
            Harvest();
            if (Ready(event)) return true;
            const auto now = std::chrono::steady_clock::now();
            if (now >= until || signal < 0) return false;
            const auto ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(until -
                                                                      now)
                    .count();
            pollfd wake{signal, POLLIN, 0};
            const int result =
                poll(&wake, 1,
                     int(std::min<int64_t>(INT_MAX, std::max<int64_t>(1, ms))));
            if (result < 0 && errno != EINTR) return false;
            if (result > 0) {
                uint64_t value;
                while (read(signal, &value, sizeof(value)) < 0 &&
                       errno == EINTR) {
                }
            }
        }
    }
    std::shared_ptr<ReadSource> source;
    std::shared_ptr<SourceDrainedWriteTarget> target;
    BoundedExecutor& writes;
    std::vector<SlotBuffer> buffers;
    std::vector<Record> records;
    std::mutex mutex;
    size_t reads_running{0}, writes_running{0};
    bool stopping{false}, initialized{false}, buffers_registered{false},
        files_registered{false};
    io_uring ring{};
    int signal{-1};
};
UringPipelineDriver::Creation UringPipelineDriver::Create(
    std::shared_ptr<ReadSource> source,
    std::shared_ptr<SourceDrainedWriteTarget> target, BoundedExecutor& writes,
    std::vector<SlotBuffer> buffers) {
    if (!source || !source->native_read_fd()) return {};
    if (!target || buffers.empty() || buffers.size() > 64)
        return {Error::kInvalidArgument, {}};
    std::vector<std::pair<uintptr_t, uintptr_t>> spans;
    for (auto buffer : buffers) {
        auto begin = reinterpret_cast<uintptr_t>(buffer.data);
        if (!begin || !buffer.capacity || buffer.capacity > UINT_MAX ||
            buffer.capacity > std::numeric_limits<uintptr_t>::max() - begin)
            return {Error::kInvalidArgument, {}};
        spans.emplace_back(begin, begin + buffer.capacity);
    }
    std::sort(spans.begin(), spans.end());
    for (size_t i = 1; i < spans.size(); ++i)
        if (spans[i].first < spans[i - 1].second)
            return {Error::kInvalidArgument, {}};
    try {
        auto impl = std::make_unique<Impl>(source, std::move(target), writes,
                                           std::move(buffers));
        if (!impl->Init(*source->native_read_fd()))
            return {Error::kNotSupported, {}};
        return {Error::kOk, std::unique_ptr<UringPipelineDriver>(
                                new UringPipelineDriver(std::move(impl)))};
    } catch (...) {
        return {Error::kIoError, {}};
    }
}
UringPipelineDriver::UringPipelineDriver(std::unique_ptr<Impl> p)
    : impl_(std::move(p)) {}
UringPipelineDriver::~UringPipelineDriver() = default;
Error UringPipelineDriver::RebindSource(
    std::shared_ptr<ReadSource> source) noexcept {
    return impl_->Rebind(std::move(source));
}
bool UringPipelineDriver::SubmitRead(const SlotToken& t, const ReadChunk& c,
                                     SlotBuffer b) noexcept {
    return impl_->Read(t, c, b);
}
bool UringPipelineDriver::SubmitWrite(const SlotToken& t, const ReadChunk& c,
                                      SlotBuffer b) noexcept {
    return impl_->Write(t, c, b);
}
void UringPipelineDriver::RequestCancel(const SlotToken& t,
                                        SlotStage s) noexcept {
    impl_->Cancel(t, s);
}
bool UringPipelineDriver::WaitForEvent(IoCompletion& c,
                                       std::chrono::milliseconds t) {
    return impl_->Wait(c, t);
}
}  // namespace mooncake::ssd_offload
#else
namespace mooncake::ssd_offload {
struct UringPipelineDriver::Impl {};
UringPipelineDriver::UringPipelineDriver(std::unique_ptr<Impl> p)
    : impl_(std::move(p)) {}
UringPipelineDriver::~UringPipelineDriver() = default;
Error UringPipelineDriver::RebindSource(std::shared_ptr<ReadSource>) noexcept {
    return Error::kNotSupported;
}
UringPipelineDriver::Creation UringPipelineDriver::Create(
    std::shared_ptr<ReadSource>, std::shared_ptr<SourceDrainedWriteTarget>,
    BoundedExecutor&, std::vector<SlotBuffer>) {
    return {};
}
bool UringPipelineDriver::SubmitRead(const SlotToken&, const ReadChunk&,
                                     SlotBuffer) noexcept {
    return false;
}
bool UringPipelineDriver::SubmitWrite(const SlotToken&, const ReadChunk&,
                                      SlotBuffer) noexcept {
    return false;
}
void UringPipelineDriver::RequestCancel(const SlotToken&, SlotStage) noexcept {}
bool UringPipelineDriver::WaitForEvent(IoCompletion&,
                                       std::chrono::milliseconds) {
    return false;
}
}  // namespace mooncake::ssd_offload
#endif
