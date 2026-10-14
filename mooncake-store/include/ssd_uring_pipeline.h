// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ssd_pipeline_io.h"

namespace mooncake::ssd_offload {
class UringPipelineDriver final : public EventPipelineDriver {
   public:
    struct Creation {
        Error error{Error::kNotSupported};
        std::unique_ptr<UringPipelineDriver> driver;
    };
    // Registers source fd and fixed buffers once for this driver; multiple
    // accepted reads share one ring. Submission and CQ polling are owned by
    // the scheduler thread; write workers only signal drained completions.
    static Creation Create(std::shared_ptr<ReadSource>,
                           std::shared_ptr<SourceDrainedWriteTarget>,
                           BoundedExecutor& writes,
                           std::vector<SlotBuffer> buffers);
    ~UringPipelineDriver() override;
    // Reuse this request's ring/fixed buffers for another source. The scheduler
    // must first drain the preceding ReadPipeline and deliver its completions.
    // Pending SQEs, unread events or write callback tails reject the change.
    // The destination target and staging-buffer ownership remain unchanged.
    Error RebindSource(std::shared_ptr<ReadSource>) noexcept;
    bool SubmitRead(const SlotToken&, const ReadChunk&,
                    SlotBuffer) noexcept override;
    bool SubmitWrite(const SlotToken&, const ReadChunk&,
                     SlotBuffer) noexcept override;
    void RequestCancel(const SlotToken&, SlotStage) noexcept override;
    bool WaitForEvent(IoCompletion&, std::chrono::milliseconds) override;

   private:
    struct Impl;
    explicit UringPipelineDriver(std::unique_ptr<Impl>);
    std::unique_ptr<Impl> impl_;
};
}  // namespace mooncake::ssd_offload
