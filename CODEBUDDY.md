# CODEBUDDY.md This file provides guidance to CodeBuddy when working with code in this repository.

## Project Overview

Mooncake is a KVCache-centric disaggregated architecture for LLM serving, open-sourced by Moonshot AI. It provides high-performance data transfer (Transfer Engine) and distributed KV cache storage (Mooncake Store) optimized for RDMA networks, with integrations into vLLM, SGLang, and other LLM inference systems.

## Build Commands

### Install Dependencies (requires root, Linux only)
```bash
sudo bash dependencies.sh -y
```
Installs system packages (libibverbs-dev, libgtest-dev, libjsoncpp-dev, libnuma-dev, etc.), yalantinglibs v0.5.7, Go 1.23.8, and initializes git submodules.

### Build from Source
```bash
mkdir build && cd build
cmake .. [OPTIONS]
make -j$(nproc)
sudo make install  # optional
```
Key CMake options: `-DUSE_CUDA=ON`, `-DUSE_TCP=ON` (default), `-DWITH_STORE=ON` (default), `-DWITH_TE=ON` (default), `-DWITH_P2P_STORE=OFF`, `-DWITH_EP=OFF`, `-DBUILD_UNIT_TESTS=ON` (default), `-DUSE_RDMA=ON`, `-DUSE_HTTP=ON` (default).

### Build Python Wheel
```bash
# After building C++ in build/
bash scripts/build_wheel.sh [python_version] [output_dir]
# e.g. bash scripts/build_wheel.sh 3.10 dist
```
Copies `.so` artifacts from `build/` into `mooncake-wheel/mooncake/`, then runs `python -m build` + `auditwheel repair`. Supports `NON_CUDA_BUILD=1` and `CU13_BUILD=1` variants.

### Run C++ Unit Tests
```bash
cd build && ctest --output-on-failure
```
Tests are GTest-based, enabled by `-DBUILD_UNIT_TESTS=ON`. Many RDMA/hardware tests are commented out by default; only TCP, metadata, topology, and common tests run without special hardware.

### Run Python Integration Tests
```bash
bash scripts/run_tests.sh
```
Launches HTTP metadata server, Transfer Engine target, mooncake_master, and runs Python tests in `mooncake-wheel/tests/`. Requires the wheel or `make install` to be done first.

### Run a Single C++ Test
```bash
cd build && ./mooncake-store/tests/<test_name>
# e.g. ./mooncake-store/tests/master_service_test
```

### Lint and Format
```bash
pip install -r requirements-dev.txt
pre-commit install
pre-commit run --all-files
```
Hooks: ruff (Python lint+format), clang-format (C/C++), cmake-format (CMake), codespell (spelling), trailing-whitespace/end-of-file-fixer. Style: Google Python and Google C++ style guides.

## Architecture

### Layer Overview

The project has a layered architecture: low-level transport → distributed storage → Python bindings → wheel packaging.

```
Python Layer (mooncake-wheel/mooncake/)
  CLI entry points, vLLM connector, Store REST service
       │ pybind11
mooncake-integration/
  engine.so (TE bindings)  │  store.so (Store bindings)
       │                        │
mooncake-transfer-engine/   mooncake-store/
  Multi-protocol transport    Distributed KV cache
       │                        │
mooncake-common/             CacheLib allocator
  Shared config, etcd        (embedded in mooncake-store)
```

### mooncake-transfer-engine (Core Transport Layer)

The Transfer Engine is the foundational data movement layer. Its C++ API lives in `mooncake-transfer-engine/include/transfer_engine.h` (`mooncake::TransferEngine` class). The key abstraction is:

1. **Init**: `init(metadata_conn_string, local_server_name)` — connects to a metadata service (HTTP, etcd, Redis, or P2P handshake).
2. **Install Transports**: `installTransport(proto, args)` — dynamically loads transport backends. Supported protocols include RDMA (InfiniBand/RoCEv2/eRDMA), TCP, CXL, NVLink (multi-node), NVMe-oF, AWS EFA, HIP (AMD), Ascend (Huawei NPU), and MUSA/MACA (Chinese GPU vendors).
3. **Memory Registration**: `registerLocalMemory(addr, length, location)` — registers local DRAM/VRAM buffers with the metadata service and transport layer.
4. **Segment Management**: `openSegment(name)` / `closeSegment(handle)` — opens remote memory segments for reading/writing.
5. **Batch Transfers**: `allocateBatchID()` → `submitTransfer(batch_id, requests)` → `getTransferStatus(batch_id, task_id)` — asynchronous batched data transfer with topology-aware path selection.

The C ABI is in `transfer_engine_c.h` for FFI access from Go/Rust. `TransferEngineImpl` (`transfer_engine_impl.h`) contains the actual implementation with metrics support (transferred bytes counter, task completion latency histogram via yalantinglibs metrics).

`TransferMetadata` (`transfer_metadata.h`) manages segment descriptors, device topology, buffer registrations, and RPC metadata. It supports metadata backends switchable via compile flags: `USE_HTTP` (default), `USE_ETCD`, `USE_REDIS`, or `P2PHANDSHAKE` (peer-to-peer mode).

Transport implementations live in `mooncake-transfer-engine/src/transport/` with each protocol in its own subdirectory. The `MultiTransport` layer (`multi_transport.h`) multiplexes across installed transports.

### mooncake-store (Distributed KV Cache)

Mooncake Store (v2.0.0) is a distributed object store specialized for LLM KV cache, built atop Transfer Engine. It follows a master-client architecture:

**MasterService** (`mooncake-store/include/master_service.h`): Central metadata manager that:
- Uses 1024-shard hash partitioning for metadata (`std::array<MetadataShard, 1024>`) with fine-grained shared mutexes.
- Implements a two-phase write protocol: `PutStart` (allocates replica buffers) → data transfer → `PutEnd`/`PutRevoke`.
- Supports multi-replica storage with `Copy`/`Move` operations and a `ClientTaskManager` for replica management tasks.
- Provides near-LRU eviction with soft-pin/hard-pin semantics and configurable high-watermark ratio.
- Supports snapshot persistence/restore, SSD offloading, and high-availability via leader election (etcd/Redis backends).

**Client** (`mooncake-store/include/client_service.h`): User-facing API with:
- `Client::Create()` factory method (connects to master, initializes Transfer Engine).
- `Get/Put/Remove/Query` operations with batch variants.
- Local hot cache (`LocalHotCache`) with frequency-based admission control (`CountMinSketch`).
- Copy/Move task creation and polling for replica management.

Memory allocation uses Facebook's CacheLib (embedded in `mooncake-store/include/cachelib_memory_allocator/`) for slab-based buffer management. An alternative `OffsetBufferAllocator` is available.

The RPC layer uses yalantinglibs C++20 coroutine-based networking (`struct_rpc`).

### mooncake-ep / mooncake-pg (Expert Parallelism)

PyTorch CUDA extensions for MoE (Mixture of Experts) model inference. Built via `BuildEpExt.cmake`/`BuildPgExt.cmake` scripts invoked during the main CMake build when `-DWITH_EP=ON`. Supports multi-PyTorch-version builds (`-DEP_TORCH_VERSIONS="2.9.1;2.8.0"`) and targets CUDA arch 8.0/9.0. The extensions are injected into the wheel after `auditwheel` to avoid patchelf corrupting CUDA fatbins.

### mooncake-integration (Python Bindings)

Bridge between C++ and Python using pybind11. Produces two shared libraries:
- `engine.so`: Transfer Engine Python API
- `store.so`: Mooncake Store Python API

Also installs Python scripts from `mooncake-wheel/mooncake/` and handles EP/PG extension symlinks.

### mooncake-wheel (Python Packaging)

The distributable Python package `mooncake-transfer-engine` (PyPI). Key entry points defined in `pyproject.toml`:
- `mooncake_master` — Store master server
- `mooncake_client` — Store client
- `transfer_engine_bench` — Transfer Engine benchmark
- `mooncake_http_metadata_server` — HTTP metadata service
- `mc_store_rest_server` — Store REST API

Important Python modules: `mooncake_connector_v1.py` (vLLM v1 connector), `mooncake_ep_buffer.py` (EP buffer management), `mooncake_store_service.py` (Store REST service).

### mooncake-common (Shared Utilities)

Shared configuration, etcd client wrapper (Go-based), and common utilities. `common.cmake` defines all compile-time feature flags and hardware acceleration options. C++ standard is C++20 with coroutines; default build type is `RelWithDebInfo`.

### Build System Design

The root `CMakeLists.txt` sets `GLOBAL_CONFIG=true` so submodules know they're in a unified build. Each submodule (e.g., `mooncake-transfer-engine`) can also build independently by detecting `NOT GLOBAL_CONFIG` and loading its own common config. Build order: pybind11 → mooncake-asio → mooncake-common → transfer-engine → store → EP/PG → integration → P2P store.

## PR Conventions

Use prefixed PR titles: `[Bugfix]`, `[CI/Build]`, `[Doc]`, `[Integration]`, `[P2PStore]`, `[Store]`, `[TransferEngine]`, `[Misc]`. Major architectural changes (>500 LOC) require an RFC GitHub issue.
