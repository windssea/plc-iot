# IoT v1 通信契约

状态：阶段 A 已实现，作为后续 Agent 与模拟端的共同输入规范。配置归属 PLC 子设备，gatewayId 表示接收快照的 PLC 采集身份。

参考校验实现现已位于 `src/plcnext_iot/contracts.py`；`tools/contract_validation.py` 保持兼容入口。这里描述的校验器本身仍不持久化消息，配置状态管理见项目的配置持久化说明。

## 1. 使用入口

[protocol.schema.json](v1/protocol.schema.json) 使用 Draft 2020-12，全部 `$ref` 为本文件内引用，无在线解析依赖。使用方按 [topics.json](v1/topics.json) 选择对应 `$defs`，不能仅通过猜测载荷形状决定消息类型。根 oneOf 只用于离线枚举全部消息形状。

参考校验流程为：限长读取 → 严格 JSON 解码 → 对应消息 Schema → 静态语义检查。只通过 JSON Schema 不等于通过完整契约。

```python
from tools.contract_validation import validate_message

issues = validate_message("configSet", payload_bytes,
                          expected_gateway_id="PLC-01")
```

`expected_gateway_id` 必须来自可信 Topic/本地 Bootstrap/认证上下文。省略它只做内容校验，不提供身份授权。返回 Issue(code, path, message)，path 为 JSON Pointer；工具最多返回 32 条问题，消息不会包含输入值。

CLI 不带参数时检查 manifest 全部样例；无效样例产生预期错误也算 PASS。单文件验证退出码：0=有效，1=无效，2=参数或文件读取错误。校验工具不建立网络连接、不应用配置。

参考库的 Schema 验证方式参见 [python-jsonschema 官方说明](https://python-jsonschema.readthedocs.io/en/v4.25.0/validate/)。本项目不依赖未经显式启用的 `format` 验证，而使用 type、pattern、条件约束及语义校验。

## 2. 公共约定

- schemaVersion 固定为 1。configVersion 为 PLC 内从 1 开始的递增安全整数；activeConfigVersion=0 表示尚无已提交配置。计数/版本/时间整数上限 9007199254740991，便于 JavaScript 安全传输。
- gatewayId/deviceId/pointId/messageId/bootId/sessionId 均为 1–64 位字符串，以字母或数字开头，其余允许字母、数字、点、下划线、横线；禁止 Topic 分隔符和通配符。部署方负责生成稳定唯一身份，样例 ID 仅作演示。
- deviceId 在同 PLC 内唯一；pointId 在同子设备内唯一，不同子设备可同名。重发同一批次不能更换 messageId。messageId 必须在当前部署全局唯一。
- JSON 仅允许 UTF-8，无 BOM，无重复键，无 NaN/Infinity 或溢出为非有限值的数字；最大嵌套深度 64。未知字段拒绝，不能将 Bootstrap 认证字段塞进业务快照。
- 没有隐式默认值注入。除明确列出的可选字段外均须填写；配置端应在发布时物化默认值，接收端不自行猜测。字符串名称不参与身份判定。
- 时间为 UTC Unix 毫秒；间隔以 Ms 结尾，uptimeSeconds 为秒。只有 LWT timestamp 明确为 null，其他消息时间必填整数。接收端单独记录 receivedAt。
- 配置字节上限 2 MiB；其他各类消息统一为 128 KiB。上限按原始 UTF-8 字节计算，含空白。设备最多 50，总点位最多 2000，遥测每批 1–500 个值。这是 v1 默认契约保护值，不是吞吐承诺。

## 3. Topic 与消息入口

前缀：`iot/v1/gateway/{gatewayId}/`。下表方向相对于 Agent，全部 QoS=1。

| 后缀 | kind / Schema 定义 | 方向 | Retain |
|---|---|---|---|
| config/set | configSet | 接收 | 是 |
| config/ack | configAck | 发布 | 否 |
| config/get | configGet | 发布 | 否 |
| data | data | 发布 | 否 |
| data/ack | dataAck | 接收 | 否 |
| status | status | 发布 | 是 |
| heartbeat | heartbeat | 发布 | 否 |
| device/status | deviceStatus | 发布 | 否 |
| event | event | 发布 | 否 |

实际 ACL 逐项生成，Agent 不能发布 config/set 或 data/ack。command Topic 未进入 v1。

## 4. PLC 完整配置快照

[有效快照](examples/valid/config-set.json) 包含 schemaVersion、messageId、gatewayId、configVersion、timestamp、config。config 内 enabled 控制全部子设备采集，report 为 PLC 公共批次设置，devices 是本 PLC 子设备的完整集合。

编辑某子设备后仍汇总整个 PLC 快照，遗漏子设备即删除。空 devices 合法。设备 points 允许为空，表示暂未配点；enabled=false 的设备/点仍必须满足所有配置约束。

Device 必填 deviceId/name/enabled/protocol/connection/points。protocol 只允许 modbus_tcp。connection 必填 host/port/unitId/connectTimeoutMs/requestTimeoutMs/retryCount：port 1–65535，unitId 0–255，超时 1–300000ms，retryCount 0–10。unitId 是 TCP Unit Identifier 字段范围，具体设备或 TCP→RTU 网关允许值需按手册确认；本契约不发送广播写请求。

Point 必填 pointId/name/enabled/dataType/pollIntervalMs/scale/offset/reportMode/reportIntervalMs/deadband/staleAfterMs/modbus；unit 可选，不填表示无单位标签。周期及 stale 间隔为 1–86400000ms，staleAfterMs ≥ pollIntervalMs。

| dataType | 区域 | 占用 | byteOrder | 转换 |
|---|---|---|---|---|
| bool | coil / discrete_input | 1 位 | 必须省略 | scale=1、offset=0、deadband=0 |
| int16 / uint16 | holding_register / input_register | 1 寄存器 | AB / BA，必填 | value=raw×scale+offset |
| int32 / uint32 / float32 | holding_register / input_register | 2 寄存器 | ABCD/BADC/CDAB/DCBA，必填 | value=raw×scale+offset |

address 使用零基偏移 0–65535，完整宽度必须落在地址空间内。40001 的传统 holding register 编号需配置端显式转换为 area=holding_register/address=0，不能在 Agent 中猜测。

reportMode 为 cyclic/change/change_or_cyclic，所有模式均填写 reportIntervalMs；change 下该值保留但不产生周期触发。deadband 为有限非负值，scale/offset 为有限数值。报告批次 maxBatchPoints 为 1–500，batchIntervalMs 为 1–86400000。

host 字段先做非空、长度和字符集检查；地址可达、DNS 解析、设备是否在线不由本工具验证。当前 v1 Schema 接受数字/字母开头的主机表示，后续如需其他地址表示须明确扩展测试。

## 5. 确认与重新请求

configAck 的 messageId 引用 config/set 的 messageId，不生成新的确认身份。包含请求的 configVersion、实际 activeConfigVersion、status 和 errors。

- APPLIED：errors 必须为空，activeConfigVersion 必须等于请求版本。
- REJECTED / FAILED：errors 至少一项，活动版本保持运行事实；本工具不假设它必然小于请求版本，低版本请求也可能被拒绝。
- Error 包含 code/path/message，deviceId/pointId 可选；禁止原样返回输入载荷和凭据。

无法解析且无法恢复合法 messageId/configVersion 的配置不伪造 configAck，应记录限流诊断事件。版本过期/同版本不同内容等检查需要持久状态，留给配置管理器。

configGet 包含新的 messageId、activeConfigVersion 和 sessionId。配置端收到后重新发送最新期望快照；它不是强制回滚当前配置的命令。

dataAck 引用原批次 messageId。STORED 必须无错误；REJECTED 必须含永久错误。数据库不可用不发 STORED，Agent 超时重试。确认是否来自授权接收端及是否对应本地在途批次，由传输和队列层检查。

## 6. 遥测、状态与事件

data 包含 messageId/bootId/sequence/configVersion/values；sequence 从 1 开始，在 bootId 内递增。一批内同 deviceId+pointId 只能出现一次。一批仅属于一个 configVersion。

每值必填 deviceId/pointId/value/quality/timestamp；sourceTimestamp 可选或 null。GOOD 的 value 必须是有限数值或布尔值，其余质量必须 null。质量：GOOD、BAD_TIMEOUT、BAD_CONNECTION、BAD_PROTOCOL、BAD_DECODE、BAD_CONFIGURATION、STALE、UNKNOWN。对照历史配置检查值类型、单位及实际点位归属属于接收端职责。

status：online、reason、sessionId 和 timestamp。connected 对应 online=true；shutdown 对应 false；connection_lost 表示 LWT，online=false 且 timestamp=null。LWT 在连接时预先准备，不把准备时刻冒充断线时刻。接收端使用自己的接收时刻/心跳超时，并进行会话关联。

device/status：携带 configVersion、sessionId 和单个 device 状态。device 内包含 deviceId、status、lastError、lastGoodTimestamp，后两者允许 null；设备状态枚举 DISABLED/CONNECTING/ONLINE/DEGRADED/OFFLINE。

heartbeat：包含 sessionId/bootId/agentVersion/activeConfigVersion/uptimeSeconds/agentState，完整 devices 状态数组，points 的 total/good/bad，queue 的 batches/payloadBytes/droppedBatches。good+bad=total，bad 表示所有非 GOOD 的已启用点，包括 UNKNOWN 和 STALE；停用点不计入。设备状态列表包含停用子设备。WAITING_CONFIG 必须版本=0、无设备及点；RUNNING/DISABLED 必须已有活动版本。队列数量可非零，例如配置清空后仍有历史补传。

event：messageId/sessionId/activeConfigVersion/level/code/message，deviceId/pointId 可选。仅提供有限枚举诊断，不提供自由对象用于远程命令或凭据传输。

## 7. 校验错误和责任边界

| 错误码 | 含义 |
|---|---|
| UNSUPPORTED_MESSAGE | 未知 kind，包括尚未支持的命令 |
| PAYLOAD_TOO_LARGE | 超过对应消息原始字节限制 |
| INVALID_JSON | UTF-8/JSON 非法、重复键、非有限数或嵌套超限 |
| SCHEMA_INVALID | 字段、类型、范围或条件约束不符 |
| GATEWAY_MISMATCH | 与调用方传入的可信 PLC 身份不一致 |
| DUPLICATE_DEVICE_ID | 子设备/设备状态身份重复 |
| DUPLICATE_POINT_ID | 同子设备内点位身份重复 |
| INVALID_ADDRESS | 完整寄存器宽度越界 |
| INVALID_STALE_INTERVAL | 过期时间短于轮询周期 |
| POINT_LIMIT_EXCEEDED | 整个 PLC 配置的总点数超限 |
| DUPLICATE_VALUE | 一批遥测内设备+点位重复 |
| ACK_VERSION_MISMATCH | APPLIED 未报告请求版本 |
| INCONSISTENT_COUNTS | 心跳点数不一致 |
| INCONSISTENT_STATE | 心跳等待配置状态与活动版本/点表不一致 |

以上为参考校验器的本地 Issue。线上 Error.code 还预留 VERSION_CONFLICT、STALE_VERSION、STORAGE_ERROR 等运行错误；不是所有本地错误都可安全构成 ACK，例如无法识别原请求身份时只能记诊断。

本工具**不验证**版本递增、同版本内容一致、真实身份绑定、在线状态、TLS/ACL、配置原子应用、ACK 是否在途、业务数据库落盘、7 天补传期限或未来时间容差。这些需要下一阶段的运行上下文。结构与静态语义校验通过不能代替这些检查。

## 8. 版本及示例维护

修改字段含义或必填条件时同步更新 Schema、样例、参考校验器和测试；破坏兼容性时增加协议版本。发布配置的 configVersion 增长不能替代 schemaVersion 变更。

现有可选字段只有 unit、sourceTimestamp、诊断定位 deviceId/pointId 及 modbus.byteOrder 的条件省略。禁止通过自动忽略未知字段获得表面兼容性。
