// Copyright 2025 Huawei Technologies Co., Ltd
// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/time.h>
#include <signal.h>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <unordered_map>
#include "common/base/status.h"
#include "transfer_engine.h"
#include "transport/transport.h"
#include "acl/acl.h"

DEFINE_string(local_server_name, "10.20.130.154:12345",
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "P2PHANDSHAKE", "etcd server host address");
DEFINE_string(mode, "initiator",
              "Running mode: initiator or target. Initiator node read/write "
              "data blocks from target node");
DEFINE_string(operation, "write", "Operation type: read or write");
DEFINE_string(protocol, "ascend_direct", "Transfer protocol: rdma|tcp|hccl");
DEFINE_string(segment_id, "10.20.130.154:12346", "Segment ID to access data");
DEFINE_int32(batch_size, 32, "Batch size");
DEFINE_uint64(block_size, 32768, "Block size for each transfer request");
DEFINE_uint64(block_iteration, 10, "number of iterations of the block");
DEFINE_bool(auto_discovery, false, "Enable auto discovery");
DEFINE_uint64(device_logicid, 0, "The device logic ID of this machine");
DEFINE_string(report_unit, "GB", "Report unit: GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb");
DEFINE_uint32(report_precision, 2, "Report precision");

using namespace mooncake;

int g_deviceLogicId = 0;
#define HOST_BUFFER_SIZE 0x1000

const static std::unordered_map<std::string, uint64_t> RATE_UNIT_MP = {
    {"GB", 1000ull * 1000ull * 1000ull},
    {"GiB", 1ull << 30},
    {"Gb", 1000ull * 1000ull * 1000ull / 8},
    {"MB", 1000ull * 1000ull},
    {"MiB", 1ull << 20},
    {"Mb", 1000ull * 1000ull / 8},
    {"KB", 1000ull},
    {"KiB", 1ull << 10},
    {"Kb", 1000ull / 8}};

static inline std::string calculateRate(uint64_t data_bytes,
                                        uint64_t duration) {
    if (!RATE_UNIT_MP.count(FLAGS_report_unit)) {
        LOG(WARNING) << "Invalid flag: report_unit only support "
                        "GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb, not support "
                     << FLAGS_report_unit
                     << " . Now use GB(default) as report_unit";
        FLAGS_report_unit = "GB";
    }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(FLAGS_report_precision)
        << 1.0 * data_bytes * 1000000 / duration /
               RATE_UNIT_MP.at(FLAGS_report_unit)
        << " " << FLAGS_report_unit << "/s";
    return oss.str();
}

// 
int allocateDevMem(void *&devAddr, size_t size) {
    // malloc device mem
    aclError ret = aclrtMalloc(&devAddr, size, ACL_MEM_MALLOC_HUGE_ONLY);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to allocate device memory, ret:" << ret;
        return ret;
    }

    // malloc host mem
    void *host_addr = nullptr;
    ret = aclrtMallocHost(&host_addr, size);
    if (ret != ACL_ERROR_NONE || host_addr == nullptr) {
        LOG(ERROR) << "Failed to allocate device memory, ret:" << ret;
        return ret;
    }

    for (size_t i = 0; i < size; i += sizeof(uint32_t)) {
        *(uint32_t *)((char *)host_addr + i) = 0x12345678;
    }

    // copy data from host mem to device mem
    ret =
        aclrtMemcpy(devAddr, size, host_addr, size, ACL_MEMCPY_HOST_TO_DEVICE);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to copy data from host to device, ret: " << ret;
        aclrtFreeHost(host_addr);
        aclrtFree(devAddr);
        return ret;
    }

    // release resource
    ret = aclrtFreeHost(host_addr);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to aclrtFreeHost, ret: " << ret;
        return ret;
    }

    return 0;
}

/**
 * initiator() - 主动端（发起端）核心逻辑
 *
 * 整体流程：
 *   阶段 1：初始化 ACL 上下文 + Transfer Engine
 *   阶段 2：分配并注册设备内存（预热用 + 各轮测试用）
 *   阶段 3：连接远端 target 的 Segment，执行一次预热传输
 *   阶段 4：循环执行多轮正式测试，每轮块大小翻倍，测量吞吐量
 *   阶段 5：释放资源
 */
int initiator() {
    // ==================== 阶段 1：初始化 ====================
    // 创建 ACL 运行时上下文，绑定到指定的 NPU 设备
    aclrtContext context = NULL;
    aclError ret = aclrtCreateContext(&context, g_deviceLogicId);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to create context, ret: " << ret;
        return ret;
    }

    // 创建 Transfer Engine 实例
    auto engine = std::make_unique<TransferEngine>(FLAGS_auto_discovery);

    // 解析本地服务名称（IP:端口），用于元数据注册
    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    std::string FLAGS_local_server_name_new =
        hostname_port.first + ":" + std::to_string(hostname_port.second);
    // 初始化 Transfer Engine：连接元数据服务，注册本节点信息
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name_new.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    // ==================== 阶段 2：分配并注册设备内存 ====================

    // --- 2a. 分配预热用的小块设备内存 ---
    // 预热传输用于建立底层 HCCS/RDMA 连接，避免首次传输的延迟计入正式测试
    void *tmp_devAddr = NULL;
    ret = allocateDevMem(tmp_devAddr, FLAGS_block_size);        // 这块的定义就在上面，先分配 NPU 内存，再在 CPU 上写一个 0x12345678，最后拷贝到 NPU 上。
    if (ret) {
        LOG(ERROR) << "Failed to allocateDevMem, ret: " << ret;
        return ret;
    }

    LOG(INFO) << "tmp_devAddr_target: " << tmp_devAddr
              << ", len: " << FLAGS_block_size;
    // 将预热内存注册到 Transfer Engine，标记为 NPU 设备内存
    // 注册后，远端节点可以通过 RDMA/HCCS 直接访问此内存
    ret = engine->registerLocalMemory(tmp_devAddr, FLAGS_block_size,
                                      "npu:" + std::to_string(g_deviceLogicId));

    // --- 2b. 为下面正式的每一轮测试分配 device 侧的内存 ---
    // 共 block_iteration 轮，第 i 轮的块大小 = block_size * 2^i
    // 每轮分配 batch_size * block_size * 2 的空间（×2 是为了隔块发送，模拟非连续内存） 
    // batchsize 是有多少个小块，blocksize 是每个小块的大小
    void *devAddr = NULL;
    std::vector<void *> g_addr;  // 保存每轮分配的设备内存起始地址
    for (uint32_t i = 0; i < FLAGS_block_iteration; i++) {
        uint64_t block_size = FLAGS_block_size * (1 << i);
        ret = allocateDevMem(devAddr, FLAGS_batch_size * block_size * 2);
        if (ret) {
            LOG(ERROR) << "Failed to allocateDevMem, ret: " << ret;
            return -1;
        }
        LOG(INFO) << "dev_addr_initiator: " << devAddr
                  << " len:" << FLAGS_batch_size * block_size * 2;
        // 将每轮的设备内存注册到 Transfer Engine
        ret = engine->registerLocalMemory(
            devAddr, FLAGS_batch_size * block_size * 2,
            "npu:" + std::to_string(g_deviceLogicId));
        if (ret) {
            LOG(ERROR) << "Failed to registerLocalMemory, ret: " << ret;
            return ret;
        }

        g_addr.push_back(devAddr);
    }

    // ==================== 阶段 3：连接远端 Segment 并执行预热传输 ====================

    // 打开远端 target 节点的 Segment（通过 segment_id 定位）
    // 这一步会触发 P2P 握手，建立与 target 的通信连接
    auto segment_id = engine->openSegment(FLAGS_segment_id.c_str());

    // 根据命令行参数确定操作类型：READ 或 WRITE
    TransferRequest::OpCode opcode;
    if (FLAGS_operation == "read")
        opcode = TransferRequest::READ;
    else if (FLAGS_operation == "write")
        opcode = TransferRequest::WRITE;
    else {
        LOG(ERROR) << "Unsupported operation: must be 'read' or 'write'";
        return -1;
    }
    LOG(INFO) << "open segment suc.";

    // 获取远端 Segment 的描述信息，其中包含远端内存 buffer 的地址列表
    auto segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    if (!segment_desc) {
        LOG(ERROR) << "Unable to get target segment ID, please recheck";
        return -1;
    }
    LOG(INFO) << "get segment desc suc.";

    // --- 执行一次预热传输 ---
    // 目的：触发底层连接建立（HCCS 链路协商、RDMA QP 创建等），排除首次连接延迟对正式测试的干扰
    Status s;
    uint64_t remote_base = 0;
    remote_base = (uint64_t)segment_desc->buffers[0].addr;  // 远端第 0 块 buffer 的地址
    auto tmp_batch_id = engine->allocateBatchID(1);          // 分配批次 ID（1 个请求）
    std::vector<TransferRequest> tmp_requests;
    TransferRequest entry;
    entry.opcode = opcode;
    entry.length = FLAGS_block_size;
    entry.source = (uint8_t *)tmp_devAddr;     // 本地源地址
    entry.target_id = segment_id;              // 远端 Segment ID
    entry.target_offset = remote_base;         // 远端目标偏移
    tmp_requests.emplace_back(entry);

    // 提交预热传输请求
    s = engine->submitTransfer(tmp_batch_id, tmp_requests);
    LOG_ASSERT(s.ok());
    LOG(INFO) << "submit transfer suc.";

    // 轮询等待预热传输完成
    // 本质上就是个 "best effort"（尽力而为）的预热，不值得因为它失败就中止整个 benchmark。
    // 打个日志，记录出错了或者超时了，但是不影响后续的 bench。
    bool completed = false;
    TransferStatus status;
    while (!completed) {
        Status s = engine->getBatchTransferStatus(tmp_batch_id, status);
        // 这里用的是 glog（Google Logging）
        LOG_ASSERT(s.ok());
        if (status.s == TransferStatusEnum::COMPLETED) {
            completed = true;
        } else if (status.s == TransferStatusEnum::FAILED) {
            LOG(ERROR) << "getTransferStatus FAILED";
            completed = true;
        } else if (status.s == TransferStatusEnum::TIMEOUT) {
            LOG(INFO) << "Sync data transfer timeout";
            completed = true;
        }
    }
    // 释放预热批次 ID，同时析构资源（delete BatchDesc）
    s = engine->freeBatchID(tmp_batch_id);
    LOG_ASSERT(s.ok());

    // ==================== 阶段 4：正式性能测试循环 ====================
    // 每轮测试：块大小 = block_size * 2^i，并发 batch_size 个请求
    // 测量总耗时并计算吞吐量
    for (uint32_t i = 0; i < FLAGS_block_iteration; i++) {
        uint64_t block_size = FLAGS_block_size * (1 << i);  // 本轮块大小

        // 记录开始时间
        struct timeval start_tv, stop_tv;
        gettimeofday(&start_tv, nullptr);

        // 获取远端第 i+1 块 buffer 的地址（第 0 块已用于预热）， buffer 来自于 target 注册
        remote_base = (uint64_t)segment_desc->buffers[i + 1].addr;

        // 分配本轮的批次 ID，容量为 batch_size 个传输请求
        auto batch_id = engine->allocateBatchID(FLAGS_batch_size);
        std::vector<TransferRequest> requests;

        // 构造 batch_size 个传输请求
        // 注意：每个请求间隔 block_size*2 的偏移（隔一块发送）
        for (int j = 0; j < FLAGS_batch_size; ++j) {
            TransferRequest entry;
            entry.opcode = opcode;
            entry.length = block_size;
            // 本地源地址：每隔 2*block_size 取一个块，故意制造非连续内存READ，更接近真实场景
            entry.source = (uint8_t *)(g_addr[i]) + block_size * 2 * j;
            entry.target_id = segment_id;
            // 远端目标地址：同样每隔 2*block_size 取一个位置 非连续 WRITE
            entry.target_offset = remote_base + block_size * 2 * j;
            requests.emplace_back(entry);
        }

        // 批量提交所有传输请求
        s = engine->submitTransfer(batch_id, requests);
        LOG_ASSERT(s.ok());

        // 轮询等待本批次所有传输完成
        bool completed = false;
        TransferStatus status;
        // （timskyzhang）TODO，报错应该退出，而不是继续计时，已经没意义了
        while (!completed) {
            Status s = engine->getBatchTransferStatus(batch_id, status);
            LOG_ASSERT(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED) {
                completed = true;
            } else if (status.s == TransferStatusEnum::FAILED) {
                LOG(ERROR) << "getTransferStatus FAILED";
                completed = true;
            } else if (status.s == TransferStatusEnum::TIMEOUT) {
                LOG(INFO) << "Sync data transfer timeout";
                completed = true;
            }
        }

        // 记录结束时间，计算耗时（微秒）
        gettimeofday(&stop_tv, nullptr);
        uint64_t duration = (stop_tv.tv_sec - start_tv.tv_sec) * 1000000.0 +
                            (stop_tv.tv_usec - start_tv.tv_usec);

        // 输出本轮测试结果：耗时、块大小、总传输量、吞吐量
        LOG(INFO) << "Test completed: duration " << duration
                  << "us, block size " << block_size / 1024 << "KB, total size "
                  << FLAGS_batch_size * block_size / 1024 << "KB , throughput "
                  << calculateRate(FLAGS_batch_size * block_size, duration);

        // 释放本轮批次 ID
        s = engine->freeBatchID(batch_id);
        LOG_ASSERT(s.ok());
    }

    // ==================== 阶段 5：释放资源 ====================
    for (uint32_t i = 0; i < FLAGS_block_iteration; i++) {
        aclrtFree(g_addr[i]);
    }
    return 0;
}

// target 运行标志位，volatile 防止编译器优化掉 while 循环中的读取
volatile bool target_running = true;

/**
 * target() - 被动端（目标端）核心逻辑
 *
 * target 的职责很简单：
 *   1. 初始化 ACL 上下文和 Transfer Engine
 *   2. 分配 NPU 设备内存并注册到 Transfer Engine
 *   3. 无限等待，让远端 initiator 通过 HCCS/RDMA 直接读写本地内存
 *
 * target 不主动发起任何传输，它只是"暴露"自己的设备内存供 initiator 访问。
 * 这正是 RDMA/HCCS 零拷贝的核心思想：target 端 CPU 无需介入数据搬运。
 */
int target() {
    // 创建 ACL 运行时上下文，绑定到指定 NPU 设备
    aclrtContext context = nullptr;
    aclError ret = aclrtCreateContext(&context, g_deviceLogicId);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to create context, ret: " << ret;
        return -1;
    }

    // 创建 Transfer Engine 实例
    auto engine = std::make_unique<TransferEngine>(FLAGS_auto_discovery);

    // 解析并初始化 Transfer Engine，注册本节点到元数据服务
    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    std::string FLAGS_local_server_name_new =
        hostname_port.first + ":" + std::to_string(hostname_port.second);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name_new.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    // --- 分配预热用的设备内存（与 initiator 端的预热传输对应） ---
    void *tmp_devAddr = NULL;
    ret = allocateDevMem(tmp_devAddr, FLAGS_block_size);
    if (ret) {
        LOG(ERROR) << "Failed to allocateDevMem, ret: " << ret;
        return ret;
    }

    LOG(INFO) << "tmp_devAddr_target: " << tmp_devAddr
              << ", len: " << FLAGS_block_size;
    // 注册预热内存到 Transfer Engine
    ret = engine->registerLocalMemory(tmp_devAddr, FLAGS_block_size,
                                      "npu:" + std::to_string(g_deviceLogicId));

    // --- 为每一轮测试分配设备内存 ---
    // 分配策略与 initiator 端完全对称：
    //   第 i 轮块大小 = block_size * 2^i
    //   每轮总大小 = batch_size * block_size * 2
    void *devAddr = NULL;
    std::vector<void *> g_addr;
    for (uint32_t i = 0; i < FLAGS_block_iteration; i++) {
        uint64_t block_size = FLAGS_block_size * (1 << i);
        ret = allocateDevMem(devAddr, FLAGS_batch_size * block_size * 2);
        if (ret) {
            LOG(ERROR) << "Failed to allocateDevMem, ret: " << ret;
            return ret;
        }

        LOG(INFO) << "devAddr_target: " << devAddr
                  << ", len: " << FLAGS_batch_size * block_size * 2;
        // 注册到 Transfer Engine，使远端 initiator 可通过 HCCS/RDMA 访问
        ret = engine->registerLocalMemory(
            devAddr, FLAGS_batch_size * block_size * 2,
            "npu:" + std::to_string(g_deviceLogicId));
        if (ret) {
            LOG(ERROR) << "Failed to registerLocalMemory, ret: " << ret;
            return ret;
        }

        g_addr.push_back(devAddr);
    }

    // 主循环：target 端进入等待状态
    // 此时远端 initiator 可以直接读写上面注册的设备内存
    // target 端 CPU 不参与任何数据搬运（零拷贝）
    while (target_running) sleep(1);

    // 收到退出信号后释放所有设备内存
    aclrtFree(tmp_devAddr);
    for (uint32_t i = 0; i < FLAGS_block_iteration; i++) {
        aclrtFree(g_addr[i]);
    }

    return 0;
}

/**
 * main() - 程序入口
 *
 * 流程：
 *   1. 解析命令行参数
 *   2. 初始化 CANN ACL 运行时环境
 *   3. 设置当前进程使用的 NPU 设备
 *   4. 根据 --mode 参数分发到 initiator() 或 target()
 */
int main(int argc, char **argv) {
    // 解析命令行参数（gflags 库）
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    // 设置全局设备 ID
    g_deviceLogicId = FLAGS_device_logicid;

    // 初始化 CANN ACL 运行时（必须在所有 ACL API 调用之前执行）
    const char *aclConfigPath = NULL;
    aclError ret = aclInit(aclConfigPath);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to initialize ACL";
        return ret;
    }

    // 设置当前线程使用的 NPU 设备（必须在 TransferEngine.init() 之前完成）
    ret = aclrtSetDevice(g_deviceLogicId);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to set device ACL";
        return ret;
    }

    // 根据运行模式分发：
    //   initiator —— 主动端，发起传输并测量性能
    //   target    —— 被动端，注册内存后等待远端访问
    if (FLAGS_mode == "initiator") {
        return initiator();
    } else if (FLAGS_mode == "target") {
        return target();
    }

    LOG(ERROR) << "Unsupported mode: must be 'initiator' or 'target'";
    exit(EXIT_FAILURE);
}