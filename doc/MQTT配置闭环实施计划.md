# MQTT 配置闭环 Implementation Plan

**Goal:** 将 v1 config/set 接入已实现的配置协调器，提供 config/ack、config/get 和连接状态，并用真实 Broker 验证。

**Architecture:** 独立 MqttSettings + PahoConnection；Paho 网络线程只接收至有界线程安全队列，asyncio 服务串行调用 AgentLifecycle。每次重连创建新 sessionId 与 LWT，SUBACK 成功后才宣告在线。配置结果转换为合法线上 ACK；未知持久化版本不伪造 ACK。

**Tech Stack:** Python 3.12、paho-mqtt==2.1.0、MQTT 3.1.1、Docker Mosquitto；沿用初始技术设计第 6/7 节。当前工作区实施，无 Hub 修改。

## Tasks

- [x] 先测试 ACK 的 APPLIED/REJECTED/FAILED、错误码映射、无效身份及未知活动版本；实现 `messaging/messages.py`。
- [x] 先测试独立 MQTT 配置的严格字段、TLS 和密码环境变量；实现 `messaging/settings.py`，不改变旧 Bootstrap 格式。
- [x] 实现 `messaging/transport.py` 的真实 Paho 客户端：连接、SUBACK、有限队列、QoS 1 发布确认和线程回收；配置服务 `messaging/config_service.py` 负责重连、身份隔离、协调器调用和状态发布。
- [x] CLI 增加 `--mqtt`，可选开启 MQTT，保持原本地模式；停止接收后等待当前处理完成并发送 offline，再回收网络线程、执行器及 SQLite。
- [x] `tools/mqtt_config_send.py` 读取显式配置文件，下发 retained 快照并等待匹配的应用 ACK；不自动编造或递增业务版本。
- [x] `tools/mqtt_smoke.py` 使用独立 Broker 验证 retained 启动、成功/重发/冲突/旧版/非法配置、断线重连及重启恢复；临时目录和动态宿主端口，完成后清理容器。
- [x] 完整测试、原 Modbus 联调回归、契约验证、依赖检查及使用说明。

## Boundaries

配置 ingress 最多 8 条、单条 2 MiB，Paho 发出队列最多 32 条。过载丢弃计数，不把 MQTT PUBACK 当成应用成功；发送方未收到 config/ack 时必须用原身份重试。Broker 应限制入站 packet size，Paho 是完整接收报文后才执行应用长度检查。

没有遥测 data/dataAck、报告/Outbox、心跳、Hub；在线状态只表示当前 MQTT 订阅可用。ACK 不新增持久化队列，发送丢失后通过原请求重发和配置版本幂等恢复。

## 验收记录（2026-09-08）

完整 unittest 85 项通过；21 个契约样例通过；独立 Mosquitto 验收通过 retained、版本保护、重连、新会话、LWT 和两个 CLI；原 Modbus 跨项目联调通过。pip check 和 Docker Compose 配置校验通过。测试容器已清理，未修改 Hub 或模拟器数据。一次早期失败测试的临时目录清理被环境策略拒绝，目录暂时保留。
