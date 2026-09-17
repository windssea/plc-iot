# Modbus 模拟采集 Implementation Plan

**Goal:** 使用 PLCnext-mock-device 的只读 Modbus TCP 服务验证 PLC 子设备采集。

**Architecture:** 沿用执行协调器；ModbusReader 负责串行协议访问，ModbusRunner 负责周期采集、连接退避和发布资格检查。不可变 Sample 进入有界本地队列，CLI 输出 JSON；尚不形成 MQTT data 报文。

**Tech Stack:** Python 3.12 asyncio、pymodbus==3.13.1、unittest；模拟器使用其自身虚拟环境。

**Spec:** IoT采集执行设计.md 第 4–6 节。本轮采用逐点读取作为最小可运行阶段，批量合并和地址拆分尚未实现。用户已指定模拟器并要求继续实现，在当前工作区执行。

## 约束

- 只调用 FC01/02/03/04，地址为零基；每设备一个客户端串行读取。
- 支持 v1 的 bool/int16/uint16/int32/uint32/float32 与全部已有字节序。
- 不成功的采样值为 None，标记 BAD_TIMEOUT/BAD_CONNECTION/BAD_PROTOCOL/BAD_DECODE。
- 启动任务不等待远端在线；采样结果在配置提交发布后才进入队列，跨版本采样丢弃。
- 轮询不补历史积压；连接故障退避，停止可取消、可重启；队列满丢最旧并计数。
- 不修改模拟器源码、已有数据库或 Hub 项目。不增加 MQTT、遥测持久化或报告策略。

## 任务

- [x] 先写 TCP 协议集成与解码失败测试，再实现 `drivers/modbus.py`：`ModbusReader.read(point)` 返回解码值，`close()` 释放连接，错误用 `ReadError.quality` 分类。
- [x] 先测试生命周期驱动采样和停机清理，再实现 `devices/modbus.py`：`ModbusRunner(device, runtime, samples)`；有界 SampleBuffer 存在 `points/samples.py`。
- [x] CLI 增加显式 `--driver modbus-tcp`，默认仍无驱动；测试参数和样本输出。运行依赖写入 `requirements-runtime.txt`。
- [x] `tools/mock_device_smoke.py` 启动指定模拟器目录中的协议适配器子进程，以标准输入命令模拟值更新、断开、重连，验证四种表、多个 Unit ID、恢复和配置切换。
- [x] 完整测试、契约回归、真实模拟器联调；记录命令及尚未实现的边界。

## 验证命令

```powershell
./.venv/Scripts/python.exe -m unittest discover -s tests -v
./.venv/Scripts/python.exe -m tools.validate_contracts
./.venv/Scripts/python.exe -m tools.mock_device_smoke --mock-project D:/work/code/PLCnext-mock-device
```

## 完成记录（2026-09-08）

已实现只读驱动、轮询任务、有界样本缓冲和 CLI；独立模拟器联调通过两个 Unit ID、四个点及断连恢复、配置切换和关闭。模拟器旧虚拟环境失效，改用 IoT 项目的 `.local/mock-venv`，未修改模拟器源码或数据库。完整 unittest 76 项通过，依赖检查和 21 个契约样例通过。
