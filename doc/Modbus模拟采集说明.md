# Modbus TCP 与模拟设备联调

本阶段实现 PLC 子设备的最小只读采集链路：配置应用 → 每设备轮询 → 解码及缩放 → 带配置版本的本地样本。联调复用 `D:/work/code/PLCnext-mock-device` 的 ModbusAdapter 和配置模型，在独立子进程中运行真实 TCP 服务。

## 运行代码

- `src/plcnext_iot/drivers/modbus.py`：串行读取、连接与请求超时、有界重试、解码。
- `src/plcnext_iot/devices/modbus.py`：每个 PLC 子设备的独立轮询任务、退避、停止及重启。
- `src/plcnext_iot/points/samples.py`：不可变 Sample 和有界本地缓冲。
- `tools/agent.py`：显式选择 `--driver modbus-tcp`，输出 sample JSON 事件。
- `tools/mock_device_smoke.py`：跨项目验收；`mock_device_server.py` 是其子进程辅助入口。

运行依赖为 `requirements-runtime.txt`。PyModbus 固定为 3.13.1，与当前模拟器一致；客户端调用按子设备串行，Unit ID 使用 `device_id` 参数。API 依据 [PyModbus 3.13.1 官方文档](https://pymodbus.readthedocs.io/en/v3.13.1/source/client.html)。目标 PLC 镜像兼容性仍需实机验证。

## 采集语义

仅调用 FC01/02/03/04，依次对应 coil、discrete_input、holding_register、input_register，地址采用零基 PDU 地址。支持 v1 的 bool、int16、uint16、int32、uint32、float32。16 位支持 AB/BA，32 位支持 ABCD/BADC/CDAB/DCBA，数值为 `raw * scale + offset`；bool 原样返回。

| 结果 | quality | value |
|---|---|---|
| 正常读取及解码 | GOOD | 工程值或 bool |
| 建连失败、建连超时、连接中断 | BAD_CONNECTION | None |
| 已连接但请求超时 | BAD_TIMEOUT | None |
| Modbus 异常响应或协议错误 | BAD_PROTOCOL | None |
| 长度不符、非有限数或转换失败 | BAD_DECODE | None |

每个设备一个客户端，逐点读取，慢请求只影响所属子设备。`retryCount` 是每次点读取遇到连接/超时错误后的额外次数，重试间隔从 100ms 开始、上限 1s。失败后设备连接退避从 500ms 增至最多 30s；退避期间按轮询周期生成坏质量样本，不反复发起网络请求。地址类协议错误不关闭健康连接。

轮询使用单调时钟，错过周期直接跳过，不补历史请求。配置 APPLIED 表示本地任务已建立，远端离线不会阻止合法配置提交。样本包含 config_version、device_id、point_id、timestamp、value、quality；timestamp 是本地读取结果产生时刻，Unix 毫秒，不是设备源时间。

采样前后都检查当前执行器身份和已发布版本。候选任务尚未发布、旧实例被替换，或读取期间发生版本切换时，丢弃该结果。报告参数变化复用执行器时，也不会把在途旧采样重新标记为新版本。

本地采样缓冲默认 1024 条，满时丢最旧并累计 droppedSamples。已入队样本保留其原配置版本，外层 activeConfigVersion 只是输出时的 Agent 状态；消费者应使用 sample.config_version。该缓冲仍是易失内存；后续阶段已提供独立 [遥测 Outbox](遥测报告与补传说明.md)，可靠补传从报告批次持久化后开始。

## 自动联调

模拟器自带虚拟环境可用时：

```powershell
./.venv/Scripts/python.exe -m pip install -r requirements-runtime.txt
./.venv/Scripts/python.exe -m tools.mock_device_smoke --mock-project D:/work/code/PLCnext-mock-device
```

本机原模拟器 `.venv` 指向已不存在的 Python 3.14。本次在 IoT 项目建立独立环境，复现命令如下（已有环境无需重建）：

```powershell
./.venv/Scripts/python.exe -m venv .local/mock-venv
./.local/mock-venv/Scripts/python.exe -m pip install -r D:/work/code/PLCnext-mock-device/requirements.lock
./.venv/Scripts/python.exe -m tools.mock_device_smoke --mock-project D:/work/code/PLCnext-mock-device --mock-python .local/mock-venv/Scripts/python.exe
```

脚本使用回环地址和临时端口、临时 Agent 数据库；不读取或修改模拟器已有数据库，也不启动 Web/MQTT/OPC UA 服务。模拟器配置经过其 AppConfig 校验，测试值通过其适配器更新接口注入。没有启动随机生成调度器，便于对期望值作精确断言。

验收包括两个 Unit ID、四类表、pressure=12.5、voltage=230、两个 bool 点；随后断开服务，确认坏质量，再重启并将 pressure 更新为 18.75，确认恢复。最后提交 v11 缩放变更，确认 voltage=23，且 meter-B 执行器保持原实例。成功输出 `result: PASS`，退出时回收子进程和连接。

## 手动采集

准备符合 v1 契约的 `.local/config.json`，将 host/port/unitId、地址、类型、字节序与模拟器页面中的点位配置对齐，再运行：

```powershell
./.venv/Scripts/python.exe -m tools.agent --bootstrap deploy/bootstrap.example.json --driver modbus-tcp --config .local/config.json --run-seconds 10
```

不传 `--run-seconds` 则持续运行。默认 `--driver none` 仍拒绝启用采集点，避免启动命令意外连接设备。已有状态库包含启用设备时，恢复启动也必须指定驱动。`sample` 是本地诊断事件，尚不是契约中的 MQTT data 消息。

## 当前边界

批量读块、非法地址拆分定位和设备状态汇总尚未实现。后续阶段已完成 [MQTT 配置闭环](MQTT配置闭环说明.md) 和 [报告、陈旧判断及遥测补传](遥测报告与补传说明.md)。采样周期极短时仅尽力执行，不保证吞吐。Hub 对接保持在独立 IoT 闭环之后。

自动测试覆盖协议请求、类型/字节序、质量分类、有限重试、停止取消、跨版本在途采样隔离、队列丢弃和 CLI 子进程。真实模拟器联调是单独命令，不作为普通单元测试的外部依赖。
