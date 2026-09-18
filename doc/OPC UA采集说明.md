# OPC UA 采集

本阶段在既有 MQTT 配置闭环上增加只读 OPC UA 子设备。配置仍按 PLC 完整快照下发；同一快照可同时包含 `modbus_tcp` 与 `opcua` 设备。采集结果进入原有报告策略、Outbox 和 data/ack 补传，不新增 Topic。

## 代码

- `src/plcnext_iot/drivers/opcua.py`：会话、Subscription、解码与质量分类。
- `src/plcnext_iot/devices/opcua.py`：子设备监督任务、退避、诊断。
- `src/plcnext_iot/devices/factory.py`：按 protocol 分发执行器。
- 依赖 `asyncua==2.0.1`，已写入 `requirements-runtime.lock`。

## 配置

设备 `protocol` 为 `opcua`。连接字段：`host`、`port`、`path`、`securityPolicy`、`securityMode`、超时与 `retryCount`。Agent 将它们拼成 `opc.tcp://{host}:{port}{path}`。`path` 可为空；对接 PLCnext-mock-device 时使用 `/iot-simulator/`。

点位使用 `opcua.nodeId`（例如 `ns=2;s=opcua-001.temperature`），并沿用统一的 `dataType`、`pollIntervalMs`、`scale`、`offset`、报告策略和 `staleAfterMs`。`pollIntervalMs` 映射为 MonitoredItem 采样周期。同一子设备内 NodeId 不得重复。

当前实现连接 `securityPolicy=None` 且 `securityMode=None`，可选 `username` + `passwordEnv`。密码只从环境变量读取，不进业务快照，初始化页也不填写南向 OPC UA 凭据；容器或 systemd 的环境文件需要提供同名变量。`Basic256Sha256` 可写入契约并成功 APPLIED，运行时该设备标记 `BAD_CONFIGURATION`，等待证书引导后再启用。

同设备重复 NodeId 在校验阶段返回 `DUPLICATE_NODE_ID`，MQTT ACK 保留该错误码。

## 运行

每个 OPC UA 子设备一个会话、一个 Subscription。连接成功后订阅启用点位，靠数据变更通知产生样本；`pollIntervalMs` 映射为 MonitoredItem 采样周期。版本切换时丢弃旧会话结果。断线退避重连（0.5–30 秒），诊断接口与 Modbus 相同，进入 `device/status` 和 heartbeat。

| 结果 | quality | value |
|---|---|---|
| 状态 Good 且类型可解码 | GOOD | 工程值或 bool |
| 建连失败、会话丢失 | BAD_CONNECTION | None |
| 已连接但请求超时 | BAD_TIMEOUT | None |
| 未知 NodeId 或 UA 状态码失败 | BAD_PROTOCOL | None |
| 类型不符或非有限数 | BAD_DECODE | None |
| 证书模式或缺少 passwordEnv | BAD_CONFIGURATION | None |

单点失败不终止同设备其他点。

```powershell
./.venv/Scripts/python.exe -m tools.agent --bootstrap deploy/bootstrap.example.json --driver opcua --config deploy/config-opcua-01.example.json --run-seconds 10
```

同时采集两种协议时使用 `--driver all`。容器和 systemd 默认已是 `all`。`--mqtt` 时 OPC UA 样本走既有遥测通道。

## 验证

```powershell
./.venv/Scripts/python.exe -m unittest discover -s tests -q
./.venv/Scripts/python.exe -m tools.validate_contracts
```

单元测试覆盖契约边界、混合快照、订阅解码、未知节点和类型错误。真机证书策略与容量需在目标 OPC UA 服务器上另行验收。
