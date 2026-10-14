#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "Usage: $0 <configured-build-dir> [--uring]" >&2
    exit 2
fi

build_dir=$1
if [[ $# -eq 2 ]]; then
    if [[ $2 != "--uring" ]]; then
        echo "Unknown option: $2" >&2
        exit 2
    fi
    export MOONCAKE_SSD_TEST_URING=1
    export MOONCAKE_OFFLOAD_USE_URING=true
    export MOONCAKE_OFFLOAD_STREAM_USE_URING=true
fi

cmake --build "$build_dir" --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-2}" --target \
    ssd_read_pipeline_test ssd_read_source_test ssd_pipeline_io_test \
    ssd_uring_pipeline_test ssd_streaming_config_test file_storage_config_test storage_backend_test \
    file_storage_test pybind_client_test ssd_destination_registry_test \
    ssd_read_protocol_test ssd_read_protocol_codec_test ssd_registered_read_test \
    ssd_peer_directory_test ssd_peer_directory_allocation_test \
    ssd_runtime_capacity_test \
    transfer_task_test client_tcp_local_memcpy_test \
    ssd_destination_registration_test secure_random_test ssd_destination_tcp_test \
    ssd_destination_rdma_test ssd_direct_read_test ssd_fixed_l2_requester_test ssd_runtime_lifetime_test \
    ssd_read_control_test ssd_control_wire_test ssd_control_rpc_test \
    ssd_owner_read_worker_test ssd_push_requester_test ssd_requester_allocation_test \
    transport_uint_test ssd_scatter_allocation_test

ctest --test-dir "$build_dir" --output-on-failure \
    -R '^(ssd_read_pipeline_test|ssd_read_source_test|ssd_pipeline_io_test|ssd_uring_pipeline_test|ssd_streaming_config_test|file_storage_config_test|ssd_destination_registry_test|ssd_read_protocol_test|ssd_read_protocol_codec_test|ssd_registered_read_test|ssd_peer_directory_test|ssd_peer_directory_allocation_test|ssd_runtime_capacity_test|transfer_task_test|client_tcp_local_memcpy_test|ssd_destination_registration_test|secure_random_test|ssd_destination_tcp_test|ssd_destination_rdma_test|ssd_direct_read_test|ssd_fixed_l2_requester_test|ssd_runtime_lifetime_test|ssd_read_control_test|ssd_control_wire_test|ssd_control_rpc_test|ssd_owner_read_worker_test|ssd_push_requester_test|ssd_requester_allocation_test|transport_uint_test|ssd_scatter_allocation_test)$'

if grep -Eq '^MOONCAKE_LIBURING_LIBRARY:FILEPATH=/.+' "$build_dir/CMakeCache.txt"; then
    cmake --build "$build_dir" --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-2}" \
        --target ssd_uring_shutdown_test
    ctest --test-dir "$build_dir" --output-on-failure -R '^ssd_uring_shutdown_test$'
fi

if grep -q '^USE_TENT:BOOL=ON$' "$build_dir/CMakeCache.txt"; then
    cmake --build "$build_dir" --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-2}" \
        --target ssd_tent_scatter_exception_test
    ctest --test-dir "$build_dir" --output-on-failure \
        -R '^ssd_tent_scatter_exception_test$'
fi

"$build_dir/mooncake-store/tests/storage_backend_test" \
    --gtest_filter='StorageBackendTest.StreamingReadSource*'
"$build_dir/mooncake-store/tests/file_storage_test" \
    --gtest_filter='FileStorageTest.OwnerStreamingUsesNativeUringInsteadOfSynchronousReadAt'
# Legacy UringFile keeps its process-wide registered arena. Run its large
# arena cases separately so a small RLIMIT_MEMLOCK does not mask native CQE.
"$build_dir/mooncake-store/tests/file_storage_test" \
    --gtest_filter='FileStorageTest.LocalStreaming*:FileStorageTest.OwnerStreaming*-FileStorageTest.OwnerStreamingUsesNativeUringInsteadOfSynchronousReadAt'
"$build_dir/mooncake-store/tests/pybind_client_test" \
    --gtest_filter='RealClientTest.LocalStreamingSsd*:RealClientTest.FixedL2*:RealClientTest.BatchGetIntoSharesDeadlineAcrossSsdOwners:RealClientTest.BatchGetDeadlineIncludesDuplicateDeviceCopy:RealClientTest.PendingDirectWrite*:RealClientTest.DirectBootstrap*:RealClientTest.TearDownSsd*'

if ctest --test-dir "$build_dir" -N -R '^ssd_fixed_l2_python_test$' | grep -q 'Total Tests: 1'; then
    cmake --build "$build_dir" --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-2}" --target store mooncake_master
    ctest --test-dir "$build_dir" --output-on-failure -R '^ssd_fixed_l2_python_test$'
fi

# --uring enables native READ_FIXED/CQE and legacy io_uring where built in.
# FileStorage and RealClient exercise classic TCP v2 end to end. The RDMA
# target suite uses CPU completion injection; RNIC correctness uses the manual
# two-host hardware script in mooncake-wheel/tests.
# Neither this runner nor --uring is an RDMA throughput result.
