#!/bin/bash
# Mooncake 集成测试脚本
# 用法: ./scripts/run_tests.sh
# 按顺序测试：Transfer Engine → Store 基础功能 → Dummy Client → Tensor → SSD 卸载 → CXL 协议 → CLI 入口点

set -e  # 任何命令失败立即退出

# 确保动态库搜索路径包含 /usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

# ============================================================
# 阶段一：Transfer Engine 传输层测试
# 测试底层跨进程内存搬运能力（TCP 模式）
# target 进程注册 64MB 缓冲区等待数据写入，
# initiator 进程执行同步单次/同步批量/异步批量读写并校验数据一致性
# ============================================================
echo "Running transfer_engine tests..."
cd mooncake-wheel/tests
# 后台启动 target（远端节点），强制走 TCP 协议
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata MC_FORCE_TCP=true python transfer_engine_target.py &
TARGET_PID=$!
# 前台运行 initiator 测试：1000 次随机读写 + 100 条批量读写 + 异步批量读写
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata MC_FORCE_TCP=true python transfer_engine_initiator_test.py
kill $TARGET_PID || true

# ============================================================
# 阶段二：Mooncake Store 分布式存储测试
# 启动 Master 服务后，依次测试：
#   - 单 Store 完整功能：CRUD、批量查询、零拷贝读写、8 线程并发压测、模糊测试
#   - 多副本功能：2 副本写入、故障容错（关闭主 Store 后从副本仍可读取）
# ============================================================
echo "Running master tests..."

# 检查 mooncake_master 是否由 pip 安装（不应出现在 /usr/local/bin）
which mooncake_master 2>/dev/null | grep -q '/usr/local/bin/mooncake_master' && \
  { echo "ERROR: mooncake_master found in /usr/local/bin, not installed by python"; exit 1; } || \
  echo "mooncake_master not found in /usr/local/bin, installed by python"

echo "mooncake_master found, running tests..."
# 设置较短的 KV Lease TTL（500ms）加速测试中的过期检测，需与客户端测试参数保持一致
mooncake_master --default_kv_lease_ttl=500 &
MASTER_PID=$!
sleep 1
# 单 Store 完整功能测试：CRUD、批量存在性查询、零拷贝 put_from/get_into、
# 8 线程×100 次×1MB 并发压测、模糊测试（1000 次随机操作 vs dict 对照）
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_distributed_object_store.py
# 多副本测试：ReplicateConfig(replica_num=2) 写入 + 故障容错读取
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_replicated_distributed_object_store.py

# ============================================================
# 阶段三：Dummy Client 轻量级客户端测试
# Dummy Client 不需要完整 Transfer Engine，通过 mooncake_client 代理操作
# 测试单客户端 CRUD + 跨客户端数据可见性（client1 写 → client2 读 → 反向验证）
# ============================================================
sleep 1
# 启动 client 代理进程
mooncake_client &
CLIENT_PID=$!
sleep 1
# 单 dummy client 的 CRUD + 批量 + 模糊测试
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_dummy_client.py
# 两个 dummy client 并行运行，互相读写验证跨客户端数据可见性
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_multi_dummy_clients.py --client-id client1 &
DUMMY_TEST_PID_1=$!
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_multi_dummy_clients.py --client-id client2 &
DUMMY_TEST_PID_2=$!
# 等待两个 client 测试都完成
wait $DUMMY_TEST_PID_1 $DUMMY_TEST_PID_2
kill $CLIENT_PID || true

# ============================================================
# 阶段四：PyTorch Tensor & Safetensor 序列化测试
# 验证 Store 对 PyTorch tensor 的直接存取能力（各种 dtype/shape），
# 以及与 .safetensors 文件格式的来回转换
# ============================================================
pip install torch numpy safetensors packaging
# Tensor 存取测试：float32/int32/bool/随机 tensor/2D·3D tensor，校验 dtype+shape+数据一致性
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_put_get_tensor.py
# Safetensor 文件序列化测试：Store 中的 tensor ↔ .safetensors 磁盘文件互转
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_safetensor_functions.py
kill $MASTER_PID || true


# ============================================================
# 阶段五（可选）：SSD 卸载驱逐测试
# 需设置 TEST_SSD_OFFLOAD_IN_EVICT 环境变量才会执行
# 向 512MB 的 segment 灌入 1000 个 1MB value（远超容量），
# 超出部分被驱逐到 SSD，再全部读回验证数据一致性
# 还包含 4 线程并发压测 + 批量 batch_get_into 验证
# ============================================================
if [ -n "$TEST_SSD_OFFLOAD_IN_EVICT" ]; then
    TEST_ROOT_DIR="/tmp/mooncake_test_ssd"
    mkdir -p $TEST_ROOT_DIR
    echo "MOONCAKE_STORAGE_ROOT_DIR is set to: $TEST_ROOT_DIR"
    echo "Running with ssd offload in evict tests..."
    # Master 启动时指定 SSD 存储根目录，用于驱逐数据的落盘
    mooncake_master --default_kv_lease_ttl=500 --root_fs_dir=$TEST_ROOT_DIR &
    MASTER_PID=$!
    sleep 1
    MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_ssd_offload_in_evict.py
    kill $MASTER_PID || true
    # 清理测试产生的临时 SSD 数据
    rm -rf $TEST_ROOT_DIR
else
    echo "Skipping test: MOONCAKE_STORAGE_ROOT_DIR environment variable is not set"
fi

# ============================================================
# 阶段六：CXL 协议传输测试
# 用 8GB 临时文件模拟 CXL 设备，走 CXL 传输协议
# 重复阶段二的所有测试用例（CRUD、零拷贝、并发、模糊测试）验证 CXL 路径正确性
# ============================================================
echo "Running CXL protocol test (test_distributed_object_store_cxl.py)..."
# 先清理之前残留的 master 进程
killall mooncake_master || true
sleep 2

echo "Starting Mooncake Master with CXL enabled (--enable_cxl=true)..."
# 启动带 CXL 支持的 Master
mooncake_master \
  --default_kv_lease_ttl=500 \
  --enable_cxl=true \
  &
CXL_MASTER_PID=$!
sleep 3
# CXL 协议下的完整功能测试
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_distributed_object_store_cxl.py
kill $CXL_MASTER_PID || true
sleep 2
echo "CXL protocol test completed successfully!"

# ============================================================
# 阶段七：CLI 入口点 + 内置 HTTP Metadata Server 测试
# 先验证 mooncake_master/client/bench 三个 CLI 命令可被发现和正常启动，
# 然后用 master 内置的 HTTP metadata server（无需单独启动）再跑一遍完整 Store 测试
# ============================================================
echo "Running CLI entry point tests..."
# CLI 工具可发现性验证 + master-client 基本通信测试
python test_cli.py

# 清理所有残留进程，准备最后一轮测试
killall mooncake_http_metadata_server || true
killall mooncake_master || true
killall mooncake_client || true
# 启动 master 并开启内置 HTTP metadata server（不需要额外启动独立的 metadata 服务）
mooncake_master --default_kv_lease_ttl=500 --enable_http_metadata_server=true &
MASTER_PID=$!
sleep 1
# 用内置 HTTP metadata server 再跑一遍完整 Store 测试，验证内置模式的正确性
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_distributed_object_store.py
sleep 1
kill $MASTER_PID || true


echo "All tests completed successfully!"
cd ../..
