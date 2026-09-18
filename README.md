# PLCnext IoT 子设备采集

在 PLCnext Linux 上运行独立 Agent，执行 PLC 子设备的数据采集配置。配置以子设备为编辑单元，按 PLC 汇总完整快照，通过 MQTT 下发。

当前交付为 **0.2.0**：独立 Agent 已形成可运行闭环。配置以子设备为编辑单元，按 PLC 汇总完整快照，经 MQTT `config/set` 下发；采集结果按报告策略写入 Outbox，经 `data` / `data/ack` 上报和补传。

已完成：契约 v1、配置事务与重启恢复、Modbus TCP 批量只读采集、OPC UA 只读订阅、MQTT 配置 ACK、遥测补传、去重接收端、设备状态/心跳、浏览器初始化、多架构镜像和未签名 WBM 开发包。已与 PLCnext-mock-device、独立 Mosquitto 联调。

尚未完成：Hub 对接、PLC / WBM 真机安装、OPC UA `Sign`/`SignAndEncrypt` 证书、南向 MQTT、写设备命令、OTA、契约中的 `event` Topic 发布。心跳里的 `agentVersion` 现为 `0.2.0`。

## 已有内容

- [初始技术设计](doc/初始技术设计.md)：定位、职责、协议及阶段安排。
- [IoT 采集执行设计](doc/IoT采集执行设计.md)：模块边界、任务隔离及故障验收。
- [通信契约](contracts/README.md)：九类消息、字段规则、错误码与验证边界。
- [实施计划](doc/通信契约实施计划.md)：本阶段任务与完成记录。
- [配置持久化说明](doc/配置持久化说明.md)：候选提交、版本保护、重启恢复和测试边界。
- [协调器与生命周期说明](doc/协调器与生命周期说明.md)：运行状态、任务切换、启动入口与驱动接口要求。
- [Modbus 模拟采集说明](doc/Modbus模拟采集说明.md)：驱动、采样语义和跨项目联调命令。
- [OPC UA 采集说明](doc/OPC UA采集说明.md)：NodeId、订阅采集、安全边界和混合快照。
- [MQTT 配置闭环说明](doc/MQTT配置闭环说明.md)：Broker 设置、下发工具、ACK 和重连验收。
- [遥测报告与补传说明](doc/遥测报告与补传说明.md)：报告策略、Outbox、STORED 确认和独立接收端。
- [批量采集与运行诊断说明](doc/批量采集与运行诊断说明.md)：读取计划、并发上限、设备状态和心跳。
- [PLCnext Linux 部署说明](doc/PLCnextLinux部署说明.md)：发布、安装、预检、升级和服务管理。
- [多架构容器镜像说明](doc/多架构容器镜像说明.md)：AMD64、ARM64、ARMv7 镜像构建、离线导入与持久卷。
- [本地初始化与 PLCnext App 部署](doc/本地初始化与PLCnextApp部署.md)：浏览器首次设置、配置查看与修改、自动保存与 WBM 安装路线。
- [WBM 安装包说明](doc/WBM安装包说明.md)：2152 / vPLC 两份未签名开发包、重建和设备验收步骤。
- [闭环联调测试方案](doc/闭环联调测试方案.md) / [测试报告](doc/闭环联调测试报告.md)：四台模拟子设备的配置、采集、状态与异常恢复。
- `contracts/v1/`：JSON Schema 和 Topic 元数据。
- `contracts/examples/`：有效/无效样例及预期结果。
- `tools/`：独立参考校验器和 CLI。
- `tests/`：契约、配置存储、生命周期、Modbus TCP、OPC UA、MQTT、遥测、初始化与部署测试。

## 本地校验

以下命令在项目根目录执行。使用 Python 3.12；这是本地开发工具环境，PLC 镜像版本需另行验证。

Windows PowerShell：

```powershell
py -3.12 -m venv .venv
./.venv/Scripts/python.exe -m pip install -r requirements-runtime.txt
./.venv/Scripts/python.exe -m unittest discover -s tests -v
./.venv/Scripts/python.exe -m tools.validate_contracts
```

若系统未注册 `py -3.12`，将首条命令换成实际 Python 3.12 解释器路径。已建立虚拟环境时无需重复创建。

Linux：

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-runtime.txt
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m tools.validate_contracts
```

验证单条 PLC 快照：

```powershell
./.venv/Scripts/python.exe -m tools.validate_contracts --kind configSet --file contracts/examples/valid/config-set.json --gateway PLC-01
```

返回 `{"valid": true, "issues": []}` 表示该消息通过结构和静态语义校验，不表示配置已被 PLC 应用。

持久化演示（临时目录，不激活设备）：

```powershell
./.venv/Scripts/python.exe -m tools.config_demo
```

本地 Agent 启动、应用空配置并退出（状态保存在 `.local/agent`）：

```powershell
./.venv/Scripts/python.exe -m tools.agent --bootstrap deploy/bootstrap.example.json --config contracts/examples/valid/config-empty.json --run-seconds 0
```

省略 `--run-seconds` 持续运行，Ctrl+C 触发有序关闭。默认不启用驱动，包含启用采集点的配置返回 `UNSUPPORTED_DRIVER`。`--driver modbus-tcp`、`--driver opcua` 或 `--driver all` 开启对应只读采集并输出本地 sample 事件。容器与 systemd 入口使用 `all`，同一配置快照可同时包含 Modbus TCP 与 OPC UA 子设备。

增加 `--mqtt deploy/mqtt.example.json` 开启 MQTT 配置通道。完整启动和下发命令见上述 MQTT 说明；自动隔离验收运行 `python -m tools.mqtt_smoke`（需 Docker）。

启用 `--mqtt` 会同时开启遥测 Outbox 和补传；接收端使用 `python -m tools.telemetry_receive`。完整端到端验收见遥测说明。

裸机 systemd 发布包、容器镜像和 WBM 开发包均已提供。容器与 systemd 默认 `--driver all`。下一步是目标 PLC 的安装与真机验收，之后再对接 Hub。




