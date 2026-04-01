"""
Transfer Engine 发起端（Initiator）测试脚本。

本脚本包含 Transfer Engine 数据传输功能的单元测试，验证发起端与目标端之间的
数据读写正确性。测试涵盖同步单次传输、同步批量传输和异步批量传输三种模式。

环境变量配置：
    TARGET_SERVER_NAME:    目标端服务地址（默认 127.0.0.1:12345）
    INITIATOR_SERVER_NAME: 发起端服务地址（默认 127.0.0.1:12347）
    MC_METADATA_SERVER:    元数据服务器地址（默认 127.0.0.1:2379）
    PROTOCOL:              传输协议类型，支持 "rdma" 或 "tcp"（默认 tcp）
    CIRCLE:                测试循环次数（默认 1000）
"""

import unittest
import os
from mooncake.engine import TransferEngine


class TestVLLMAdaptorTransfer(unittest.TestCase):
    """Transfer Engine 数据传输功能测试类。

    测试发起端（Initiator）通过 Transfer Engine 向目标端（Target）进行
    数据写入和读取操作的正确性，包括同步传输、批量传输和异步传输。
    """

    # 默认缓冲区大小：64MB
    DEFAULT_BUFFER_CAPACITY = 64 * 1024 * 1024

    @classmethod
    def setUpClass(cls):
        """测试类初始化：配置服务地址并创建 Transfer Engine 发起端实例。"""
        # 从环境变量读取各服务地址和协议配置
        cls.target_server_name = os.getenv("TARGET_SERVER_NAME", "127.0.0.1:12345")
        cls.initiator_server_name = os.getenv(
            "INITIATOR_SERVER_NAME", "127.0.0.1:12347"
        )
        cls.metadata_server = os.getenv("MC_METADATA_SERVER", "127.0.0.1:2379")
        cls.protocol = os.getenv("PROTOCOL", "tcp")  # 传输协议类型："rdma" 或 "tcp"
        cls.circle = int(os.getenv("CIRCLE", 1000))  # 测试循环次数

        # 创建并初始化 Transfer Engine 发起端实例
        cls.adaptor = TransferEngine()
        ret = cls.adaptor.initialize(
            cls.initiator_server_name, cls.metadata_server, cls.protocol, ""
        )
        if ret != 0:
            raise RuntimeError(f"发起端初始化失败，错误码：{ret}")

        # 如果使用 P2P 握手模式，需要用实际分配的 RPC 端口更新服务地址
        if cls.metadata_server == "P2PHANDSHAKE":
            cls.initiator_server_name = (
                cls.initiator_server_name.rpartition(":")[0]
                + ":"
                + str(cls.adaptor.get_rpc_port())
            )

        # 备用缓冲区地址，用于记录是否分配了托管缓冲区（便于清理）
        cls._fallback_buffer_addr = None

    @classmethod
    def _ensure_buffer_available(cls):
        """确保发起端有可用的内存缓冲区。

        首先尝试获取已注册的缓冲区地址；若不存在，则分配一个托管缓冲区作为备用。

        Returns:
            int: 可用缓冲区的内存地址。
        """
        src_addr = cls.adaptor.get_first_buffer_address(cls.initiator_server_name)
        if src_addr == 0:
            # 没有已注册的缓冲区，分配托管缓冲区
            cls._fallback_buffer_addr = cls.adaptor.allocate_managed_buffer(
                cls.DEFAULT_BUFFER_CAPACITY
            )
            if cls._fallback_buffer_addr == 0:
                raise RuntimeError("分配备用缓冲区失败")
            src_addr = cls._fallback_buffer_addr
        return src_addr

    @classmethod
    def tearDownClass(cls):
        """测试类清理：释放备用缓冲区（如果已分配）。"""
        if cls._fallback_buffer_addr is not None:
            cls.adaptor.free_managed_buffer(
                cls._fallback_buffer_addr, cls.DEFAULT_BUFFER_CAPACITY
            )
            cls._fallback_buffer_addr = None

    def test_random_write_circle_times(self):
        """测试同步单次传输：循环多次随机字符串的写入和读取验证。

        流程（每次循环）：
        1. 生成随机字符串并写入本地缓冲区
        2. 通过同步写操作将数据传输到远端目标
        3. 清空本地缓冲区
        4. 通过同步读操作从远端读回数据
        5. 校验读回的数据与原始数据是否一致
        """
        import random
        import string

        def generate_random_string(length):
            """生成指定长度的随机字符串（包含字母、数字和标点符号）。"""
            chars = string.ascii_letters + string.digits + string.punctuation
            return "".join(random.choices(chars, k=length))

        adaptor = self.adaptor
        circles = self.circle

        # 获取本地和远端的缓冲区地址
        src_addr = self._ensure_buffer_available()
        dst_addr = adaptor.get_first_buffer_address(self.target_server_name)
        self.assertNotEqual(dst_addr, 0, "目标端没有已注册的缓冲区")

        for i in range(circles):
            # 生成长度随机（16~256）的随机字符串
            str_len = random.randint(16, 256)
            src_data = generate_random_string(str_len).encode("utf-8")
            data_len = len(src_data)

            # 步骤1：将随机数据写入本地缓冲区
            result = adaptor.write_bytes_to_buffer(src_addr, src_data, data_len)
            self.assertEqual(result, 0, f"[{i}] 写入本地缓冲区失败")

            # 步骤2：同步写——将本地缓冲区数据传输到远端目标
            result = adaptor.transfer_sync_write(
                self.target_server_name, src_addr, dst_addr, data_len
            )
            self.assertEqual(result, 0, f"[{i}] 同步写传输失败")

            # 步骤3：清空本地缓冲区（用全零覆盖）
            clear_data = bytes([0] * data_len)
            result = adaptor.write_bytes_to_buffer(src_addr, clear_data, data_len)
            self.assertEqual(result, 0, f"[{i}] 清空本地缓冲区失败")

            # 步骤4：同步读——从远端目标读回数据到本地缓冲区
            result = adaptor.transfer_sync_read(
                self.target_server_name, src_addr, dst_addr, data_len
            )
            self.assertEqual(result, 0, f"[{i}] 同步读传输失败")

            # 步骤5：校验数据一致性
            read_back = adaptor.read_bytes_from_buffer(src_addr, data_len)
            self.assertEqual(read_back, src_data, f"[{i}] 数据校验不一致")

        print(f"[✓] {circles} 次随机写读测试全部通过。")

    def test_batch_write_read(self):
        """Test batch_transfer_sync_write and batch_transfer_sync_read for batch write/read consistency."""
        import random
        import string

        def generate_random_string(length):
            chars = string.ascii_letters + string.digits + string.punctuation
            return "".join(random.choices(chars, k=length))

        adaptor = self.adaptor
        batch_size = 100  # Adjust batch size if needed
        circles = max(2, self.circle // 100)  # Number of batch test rounds

        base_src_addr = self._ensure_buffer_available()
        base_dst_addr = adaptor.get_first_buffer_address(self.target_server_name)
        self.assertNotEqual(base_dst_addr, 0, "Target server has no registered buffers")

        src_addr_list = []
        dst_addr_list = []
        offset_size = 1024  # 1KB offset between each buffer

        for i in range(batch_size):
            src_addr_list.append(base_src_addr + i * offset_size)
            dst_addr_list.append(base_dst_addr + i * offset_size)

        for i in range(circles):
            # Generate multiple groups of random data
            data_list = []
            data_len_list = []
            for _ in range(batch_size):
                str_len = random.randint(32, min(128, offset_size))
                src_data = generate_random_string(str_len).encode("utf-8")
                data_list.append(src_data)
                data_len_list.append(len(src_data))

            # Write to local buffers in batch
            for j in range(batch_size):
                result = adaptor.write_bytes_to_buffer(
                    src_addr_list[j], data_list[j], data_len_list[j]
                )
                self.assertEqual(result, 0, f"[{i}-{j}] writeBytesToBuffer failed")

            # Batch write to remote
            result = adaptor.batch_transfer_sync_write(
                self.target_server_name, src_addr_list, dst_addr_list, data_len_list
            )
            self.assertEqual(result, 0, f"[{i}] batch_transfer_sync_write failed")

            # Clear local buffers
            for j in range(batch_size):
                clear_data = bytes([0] * data_len_list[j])
                result = adaptor.write_bytes_to_buffer(
                    src_addr_list[j], clear_data, data_len_list[j]
                )
                self.assertEqual(result, 0, f"[{i}-{j}] Clear buffer failed")

            # Batch read back from remote
            result = adaptor.batch_transfer_sync_read(
                self.target_server_name, src_addr_list, dst_addr_list, data_len_list
            )
            self.assertEqual(result, 0, f"[{i}] batch_transfer_sync_read failed")

            # Verify data consistency
            for j in range(batch_size):
                read_back = adaptor.read_bytes_from_buffer(
                    src_addr_list[j], data_len_list[j]
                )
                self.assertEqual(
                    read_back, data_list[j], f"[{i}-{j}] Data mismatch in batch read"
                )

        print(
            f"[✓] {circles} rounds of batch_write_read passed, batch size {batch_size}."
        )

    def test_async_batch_write_read(self):
        """测试异步批量传输：使用 batch_transfer_async_write/read 进行异步批量写读一致性验证。

        与同步批量传输不同，异步传输会立即返回一个 batch_id，
        需要通过 get_batch_transfer_status 轮询等待传输完成。

        流程（每轮循环）：
        1. 为每个批量槽位生成随机数据并写入本地缓冲区
        2. 提交异步批量写任务，获取 batch_id
        3. 等待异步写任务完成
        4. 清空所有本地缓冲区
        5. 提交异步批量读任务，获取 batch_id
        6. 等待异步读任务完成
        7. 逐一校验每个槽位的数据一致性
        """
        import random
        import string

        def generate_random_string(length):
            """生成指定长度的随机字符串。"""
            chars = string.ascii_letters + string.digits + string.punctuation
            return "".join(random.choices(chars, k=length))

        adaptor = self.adaptor
        batch_size = 100  # 每批传输的数据块数量
        circles = max(2, self.circle // 100)  # 异步批量测试的轮数

        # 获取本地和远端的基准缓冲区地址
        base_src_addr = self._ensure_buffer_available()
        base_dst_addr = adaptor.get_first_buffer_address(self.target_server_name)
        self.assertNotEqual(base_dst_addr, 0, "目标端没有已注册的缓冲区")

        # 构建批量传输的地址列表，每个槽位偏移 1KB
        src_addr_list = []
        dst_addr_list = []
        offset_size = 1024  # 每个缓冲区槽位之间的偏移量：1KB

        for i in range(batch_size):
            src_addr_list.append(base_src_addr + i * offset_size)
            dst_addr_list.append(base_dst_addr + i * offset_size)

        for i in range(circles):
            # 为每个槽位生成随机数据
            data_list = []
            data_len_list = []
            for _ in range(batch_size):
                str_len = random.randint(32, min(128, offset_size))
                src_data = generate_random_string(str_len).encode("utf-8")
                data_list.append(src_data)
                data_len_list.append(len(src_data))

            # 步骤1：将各组随机数据分别写入对应的本地缓冲区槽位
            for j in range(batch_size):
                result = adaptor.write_bytes_to_buffer(
                    src_addr_list[j], data_list[j], data_len_list[j]
                )
                self.assertEqual(result, 0, f"[{i}-{j}] 写入本地缓冲区失败")

            # 步骤2：提交异步批量写任务
            batch_id = adaptor.batch_transfer_async_write(
                self.target_server_name, src_addr_list, dst_addr_list, data_len_list
            )
            self.assertNotEqual(
                batch_id,
                0,
                f"[{i}] 异步批量写任务提交失败，batch_id={batch_id}",
            )

            # 步骤3：等待异步写任务完成
            result = adaptor.get_batch_transfer_status([batch_id])
            self.assertEqual(
                result, 0, f"[{i}] 异步写传输执行失败，batch_id={batch_id}"
            )

            # 步骤4：清空所有本地缓冲区（用全零覆盖）
            for j in range(batch_size):
                clear_data = bytes([0] * data_len_list[j])
                result = adaptor.write_bytes_to_buffer(
                    src_addr_list[j], clear_data, data_len_list[j]
                )
                self.assertEqual(result, 0, f"[{i}-{j}] 清空本地缓冲区失败")

            # 步骤5：提交异步批量读任务
            batch_id = adaptor.batch_transfer_async_read(
                self.target_server_name, src_addr_list, dst_addr_list, data_len_list
            )
            self.assertNotEqual(
                batch_id,
                0,
                f"[{i}] 异步批量读任务提交失败，batch_id={batch_id}",
            )

            # 步骤6：等待异步读任务完成
            result = adaptor.get_batch_transfer_status([batch_id])
            self.assertEqual(
                result, 0, f"[{i}] 异步读传输执行失败，batch_id={batch_id}"
            )

            # 步骤7：逐一校验每个槽位的数据一致性
            for j in range(batch_size):
                read_back = adaptor.read_bytes_from_buffer(
                    src_addr_list[j], data_len_list[j]
                )
                self.assertEqual(
                    read_back, data_list[j], f"[{i}-{j}] 异步批量读数据校验不一致"
                )

        print(
            f"[✓] {circles} 轮异步批量写读测试全部通过，每批 {batch_size} 个数据块。"
        )


if __name__ == "__main__":
    unittest.main()
