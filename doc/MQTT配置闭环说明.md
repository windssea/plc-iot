# MQTT 配置下发与确认

当前可通过 MQTT 下发 PLC 的完整子设备快照，并收到配置协调器完成后的应用 ACK。仍由子设备编辑配置、按 PLC 汇总；本阶段不修改 Hub，也不增加设备层级。

## 运行顺序

Agent 先恢复 SQLite 已提交配置和采集任务，再启动 MQTT 服务。Broker 不可用时本地任务继续运行。每次连接使用新的 sessionId，设置携带该身份的 retained LWT；只有收到所有订阅的 QoS 1 SUBACK，才发布在线状态和 config/get。订阅被拒绝或降级为 QoS 0 时不宣告在线。

以 PLC-01 为例：

| Topic | 行为 |
|---|---|
| `iot/v1/gateway/PLC-01/config/set` | QoS 1 订阅，接收完整快照；发送方使用 retain=true |
| `iot/v1/gateway/PLC-01/config/ack` | QoS 1、非 retained，携带原 messageId/configVersion |
| `iot/v1/gateway/PLC-01/config/get` | QoS 1、非 retained，连接订阅成功后及每 30 秒请求配置 |
| `iot/v1/gateway/PLC-01/status` | QoS 1、retained，connected/shutdown/connection_lost |

配置消息进入已有串行协调器，完成 prepare → activate → commit → publish 后才发送 APPLIED。同版同内容重发可重复确认，冲突或旧版保持当前活动配置。无驱动、执行失败和存储错误都会转换成契约允许的错误，不能把本地诊断码直接复制到线上。

JSON 损坏、重复键、超长报文、外来 gatewayId 或无效关联身份，不发送猜测的 ACK。有效关联身份下的非法配置返回 REJECTED。无法确认数据库活动版本时不发送伪造版本的 ACK。响应错误说明使用固定文本，不回显原配置或异常内容。

断线后以 1–30 秒退避创建新客户端并重新订阅，Broker 会重放 retained 配置。每次连接使用新的 sessionId。正常关闭停止接收新配置，等待当前处理完成，尝试确认 offline 发布，再断开 MQTT、停止采集和关闭 SQLite。非正常断线由 Broker 发布 LWT，timestamp=null，由接收端记录实际收到时刻。

## MQTT 设置

旧 Bootstrap 四字段不变，新增可选 `--mqtt` 指向单独文件。`deploy/mqtt.example.json` 是本机匿名明文联调配置。支持的字段如下，未知字段、重复键和密码明文字段会被拒绝。

| 字段 | 默认/说明 |
|---|---|
| host | 必填 Broker 主机名或 IP |
| port | 默认 8883，1–65535 |
| tls | 默认 true；使用系统 CA 和主机名校验 |
| caFile | 可选自定义 CA，相对 MQTT 设置文件解析；要求 TLS |
| username / passwordEnv | 成对提供；密码从指定环境变量读取，不保存在配置文件或对象 repr 中 |
| keepalive | 默认 30 秒，10–300 |

Paho 固定为 2.1.0，使用 Callback API v2 和 MQTT 3.1.1。网络循环运行在专用线程，接收回调只写入有界队列，SQLite 操作仍由现有 StoreWorker 执行。发布等待 PUBACK 后才能视为 Broker 已接收；PUBACK 不等于配置已应用。这些 API 的语义见 [Paho 官方客户端文档](https://eclipse.dev/paho/files/paho.mqtt.python/html/client.html)。

## 手动闭环

在仓库根目录安装依赖并启动本地测试 Broker：

```powershell
./.venv/Scripts/python.exe -m pip install -r requirements-runtime.txt
docker compose -f deploy/compose.mqtt-test.yml up -d
```

启动 Agent（单独终端，首次无配置时等待下发）：

```powershell
./.venv/Scripts/python.exe -m tools.agent --bootstrap deploy/bootstrap.example.json --mqtt deploy/mqtt.example.json
```

另开终端下发空配置：

```powershell
./.venv/Scripts/python.exe -m tools.mqtt_config_send --mqtt deploy/mqtt.example.json --gateway PLC-01 --config contracts/examples/valid/config-empty.json
```

工具先订阅 ACK，再发布 retained 配置；最多等待 30 秒，每 5 秒使用原始报文重发，匹配 gatewayId/messageId/configVersion 并校验 ACK。退出码 0 仅表示收到 APPLIED，1 表示拒绝、失败或未确认，2 表示输入错误。可用 `--timeout` 调整总等待时长。连接中断时本次调用返回未确认，可重新运行相同命令。

工具不自动递增版本、不分配业务 messageId，也不修改文件；发布方应保存配置文件，用新的更高版本发布变更，重试时保留原身份。已有数据库版本高于示例 v10 时，示例会被按版本规则拒绝。测试 Broker 未开启持久化，重建后 retained 配置丢失，需要配置端重新下发；Agent 已提交配置仍从本地恢复。

需要采集模拟设备时，在 Agent 命令增加 `--driver all`（或 `--driver modbus-tcp` / `--driver opcua`），并下发与模拟器点位匹配的完整配置。启用 MQTT 时同时发布 data、心跳和设备状态，并等待 data/ack=STORED；见 [遥测报告与持久补传](遥测报告与补传说明.md) 和 [批量采集与运行诊断](批量采集与运行诊断说明.md)。

联调完成后：

```powershell
docker compose -f deploy/compose.mqtt-test.yml down
```

## 自动验收

```powershell
./.venv/Scripts/python.exe -m tools.mqtt_smoke
```

要求 Docker 可用。脚本使用固定版本 `eclipse-mosquitto:2.0.22`，独立随机名称容器、回环地址临时端口、临时 Agent 数据库，验证完毕关闭线程/连接并删除容器。端口在容器重启期间保持不变。测试不连接现有 Broker，不使用 Hub，也不改模拟器已有状态。

验收覆盖 retained 启动、APPLIED、重复下发、版本冲突、旧版拒绝、非法配置、无驱动拒绝、正常 offline、LWT、Agent 重启、Broker 重启、新 sessionId、停用配置，以及真实 CLI 下发/恢复。

## 有界资源与当前限制

每个连接最多缓存 8 条入站消息，单条应用上限 2 MiB；满队列、超长或非订阅 Topic 计入丢弃数。Paho 发出队列最多 32 条、最多 8 条在途。接收线程不会为每条消息无限创建 asyncio 任务。入站丢弃计数只在进程内累计，不进入心跳；心跳 `queue` 统计的是遥测 Outbox，不是 MQTT 入站丢弃。

MQTT 的 PUBACK 只确认传输；满队列丢弃时也可能已有 PUBACK，发送方必须以 config/ack 为准并重试。Paho 会先完整接收报文，再执行应用长度检查，所以 Broker 也必须限制最大 packet size；测试配置已设置该上限。

配置 ACK 尚无独立持久 Outbox，发送中断后依靠配置重发与存储幂等恢复。传输使用 clean_session=true，不依赖持久订阅队列。connect 超时设为 3 秒、SUBACK/PUBACK 等待 5 秒；系统 DNS 和线程退出仍受操作系统影响，不是进程硬终止期限。

TLS 保持证书/主机名校验，但本地联调只验收了明文 Broker；生产证书、认证和 Broker Topic ACL 需在部署环境验证。`status` online 仅表示 MQTT 配置通道可用，不表示现场设备在线。契约中的 `event` Topic 已定义，Agent 尚未发布。Hub 对接仍未开始。
