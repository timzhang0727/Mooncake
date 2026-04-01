"""
Transfer Engine 目标端（Target）服务脚本。

本脚本启动一个 Transfer Engine 目标端实例，用于接收来自发起端（Initiator）的数据传输请求。
目标端会注册内存缓冲区，并以常驻进程的方式持续运行，等待远程读写操作。

环境变量配置：
    TARGET_SERVER_NAME:    目标端服务地址（默认 127.0.0.1:12345）
    INITIATOR_SERVER_NAME: 发起端服务地址（默认 127.0.0.1:12347）
    MC_METADATA_SERVER:    元数据服务器地址（默认 127.0.0.1:2379）
    PROTOCOL:              传输协议类型，支持 "rdma" 或 "tcp"（默认 tcp）
"""

import os
from mooncake.engine import TransferEngine      # 这里 TE 是 cpp 写的，所以跳转不过去，代码在：mooncake-integration/transfer_engine/transfer_engine_py.cpp

# 从环境变量读取各服务地址和传输协议配置
target_server_name = os.getenv("TARGET_SERVER_NAME", "127.0.0.1:12345")
initiator_server_name = os.getenv("INITIATOR_SERVER_NAME", "127.0.0.1:12347")
metadata_server = os.getenv("MC_METADATA_SERVER", "127.0.0.1:2379")
protocol = os.getenv("PROTOCOL", "tcp")  # 传输协议类型："rdma" 或 "tcp"

# 默认缓冲区大小：64MB
DEFAULT_BUFFER_CAPACITY = 64 * 1024 * 1024

# 创建并初始化 Transfer Engine 目标端实例
target = TransferEngine()
ret = target.initialize(target_server_name, metadata_server, protocol, "")
if ret != 0:
    raise RuntimeError(f"目标端初始化失败，错误码：{ret}")

# 尝试获取已注册的缓冲区地址；若不存在则分配一个托管缓冲区作为备用
# mooncake-integration/transfer_engine/transfer_engine_py.cpp Line 933
# 这里只是取地址，这个 target 节点只是被初始化（安装协议+链接metadata+创建 segment）。而初始化并不负责注册内存，因此这里做 get_address 是有可能拿到 0 的
buffer_addr = target.get_first_buffer_address(target_server_name)

if buffer_addr == 0:
    # 当前没有已注册的缓冲区，分配托管缓冲区作为备用
    buffer_addr = target.allocate_managed_buffer(DEFAULT_BUFFER_CAPACITY)
    if buffer_addr == 0:
        raise RuntimeError("为目标端分配备用缓冲区失败")
    print(f"目标端已分配备用缓冲区，地址：{buffer_addr}")

print(f"目标端服务已就绪，监听地址：{target_server_name}")

# 主循环：保持进程持续运行，等待远程传输请求
while True:
    pass
