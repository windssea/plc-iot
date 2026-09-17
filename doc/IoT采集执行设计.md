# IoT 采集执行设计

版本：0.1 · 2026-09-07 · 设计状态：待进入编码

本文细化《初始技术设计》，聚焦 PLC 子设备的采集执行。Hub 仅作为未来配置端和接收端，本文不规定其页面或表结构。

## 1. 配置从子设备到 PLC 的转换

子设备采集配置包含：所属 PLC 身份、子设备身份、启用状态、协议连接参数、点位列表。所属 PLC 用于配置端分组，不由现场设备自行声明。

配置端维护两种不同状态：可编辑草稿和不可变发布快照。不能直接将任意草稿变化广播到 PLC。

一次发布流程：

1. 读取目标 PLC 下全部子设备的同一修订视图，避免 A 的新配置与 B 的旧配置被无意拼接。
2. 转换为统一 DeviceConfig，校验子设备与点位身份及协议字段。
3. 对比上次发布内容，显示新增、修改、禁用、删除清单。
4. 分配该 PLC 的新 configVersion，持久保存快照后发布。
5. 根据配置 ACK 更新该次发布结果；编辑中的草稿不影响在途发布内容。

例如 PLC-01 管理 meter-A 与 meter-B：只把 meter-A 的周期从 1s 改为 500ms，发布仍包含 A 和 B；Agent 重新生成 A 的计划，B 继续运行。PLC-02 的版本、连接及任务不受影响。

同一个 PLC 内快照整体验证、整体提交；一处配置错误拒绝整个候选版本，旧版本继续运行。运行中单个点通信失败只影响该点或读块，不能混同为配置事务失败。

## 2. 建议工程结构

```text
PLCnext-iot/
├── doc/                         设计及协议说明
├── contracts/                   后续 JSON Schema 和有效/无效协议样例
├── src/plcnext_iot/
│   ├── core/                    启动、设置、进程生命周期
│   ├── config/                  配置模型、校验、diff、应用事务
│   ├── devices/                 子设备注册表、监督任务、状态
│   ├── drivers/                 协议接口、工厂、Modbus TCP
│   ├── points/                  读块计划、解码、统一点值
│   ├── reporting/               最新值、变化检测、批次
│   ├── messaging/               MQTT 传输、Topic、消息编解码
│   ├── storage/                 SQLite、配置存储、遥测队列
│   └── health/                  运行诊断、资源指标
├── tools/simulator/             配置/接收端和 Modbus 模拟设备
├── deploy/                      后续镜像与本地模拟环境
└── tests/                       单元、集成、故障验收
```

以上为拟建目录，本次不创建空模块占位。MVP 不提前实现 OPC UA、南向 MQTT、PLC Bridge 的空类。

## 3. 模块契约

| 模块 | 输入 | 输出 | 依赖与职责边界 |
|---|---|---|---|
| ConfigCodec | 限长 JSON 字节 | 验证后的 Snapshot 或字段错误 | 纯数据转换，不启动连接 |
| ConfigPlanner | 活动/候选 Snapshot | DeviceChangeSet、ReadPlan | 纯计算，不改数据库 |
| ConfigCoordinator | 候选快照 | ApplyResult | 串行控制配置持久化与任务切换 |
| DeviceSupervisor | DeviceConfig、generation | 带代次的采样/状态事件 | 管理一台子设备生命周期 |
| ModbusDriver | ConnectionConfig、ReadBlock | 原始字节/位值或通信错误 | 不知道 MQTT、Hub、SQLite |
| PointProcessor | 原始结果、PointConfig | PointValue | 字节序、类型、缩放、质量 |
| ReportEngine | PointValue、报告时钟 | 待持久化 Batch | 不直接调用 MQTT |
| OutboxStore | 不可变 Batch | 已入队记录/容量错误 | 唯一保存、重试、确认删除入口 |
| NorthboundTransport | Topic 与消息字节 | 发布状态/入站消息 | 传输层，不决定配置是否应用 |
| Diagnostics | 状态事件和计数器 | 心跳、诊断快照 | 不根据遥测补传推断在线 |

所有公共事件为不可变对象；跨层传错误码及上下文，不以捕获任意异常后填零作为成功。

内部 SampleEvent 至少包含 generation、deviceId、pointId、timestamp、value、quality。报文 configVersion 在形成批次时确定；一个批次不能混入不同版本的点值。

## 4. 调度和连接

单个 asyncio 主循环维护设备监督任务、配置任务、报告任务、MQTT 任务及健康任务。设备异常在监督任务内处理，不能冒泡取消所有设备。

同一子设备不同周期组成不同 ReadPlan。计划时钟使用 monotonic，采样时刻使用 UTC；调整系统时钟不造成轮询间隔突变。

单个子设备连接内最多一个在途请求。相同 host/port 下不同 unitId 的设备可能共享一个物理网关，因此增加 endpoint 级限流键 `(host, port)`，默认串行；不同 endpoint 可并发。MVP 先保留每子设备客户端，连接复用后续按实测决定。

全局并发预算建议从 16 开始，可通过 Bootstrap 调低；设备任务及队列都受容量限制。等待并发槽位期间不积累每轮请求对象，轮次过期直接记录 skipped_poll。

请求超时与轮询周期独立。慢设备不强行补齐历史轮询，诊断显示实际周期、超期次数和读延迟。连接错误按退避重连，协议地址错误不反复断开健康 TCP 连接。

## 5. Modbus 读块与解码

首期只读 FC01/02/03/04，对应 coil、discrete_input、holding_register、input_register。读寄存器每请求不超过 125 个，读位每请求不超过 2000 个；设备允许更小上限，最终采用协议限制与设备配置的较小值。地址为 0–65535，必须校验 address + width - 1 不越界。依据 [Modbus 官方应用协议 V1.1b3](https://modbus.org/docs/Modbus_Application_Protocol_V1_1b3.pdf)。

计划按区域、周期分组并排序，默认只合并连续或重叠范围。uint16/int16 占 1 寄存器，uint32/int32/float32 占 2 寄存器，bool 占 1 位。不同解释的重叠地址允许共享读取结果，但 pointId 必须唯一。

示例：address 0 的 uint16、address 1 的 uint16 和 address 2 的 float32，均为 holding_register/1000ms，可生成 start=0、quantity=4 的读块；address 10 的点另建块，不默认跨未定义地址。

一个浮点值不得跨读块边界。字节序解码用已知向量测试：float32 的 1.0 大端字节为 3F 80 00 00；四种线序分别构造输入并还原同值。协议响应长度不足标 BAD_PROTOCOL；解码后非有限数标 BAD_DECODE，不能输出 JSON NaN/Infinity。

批量读取返回非法地址时，按预先已知点范围拆分重试以定位，单周期额外请求最多 4 次，未定位部分标坏质量并留待后续诊断。网络超时不执行这种拆分，避免故障放大。

## 6. 配置切换与缓存代次

候选配置中各设备的变更分类：ADD、REMOVE、CONNECTION_CHANGED、POINT_PLAN_CHANGED、REPORT_ONLY、UNCHANGED。

- CONNECTION_CHANGED 需关闭旧连接，启用新连接参数。
- POINT_PLAN_CHANGED 替换该子设备计划，保持身份，清理受影响点旧缓存。
- REPORT_ONLY 仅替换报告策略，不重新连接设备。
- REMOVE 停任务并移除最新值缓存，已持久化旧批次保留用于补传。
- UNCHANGED 保持任务运行；提交后更新其输出所属的活动配置上下文。

ConfigCoordinator 用短暂提交屏障切换活动版本：屏障前的结果属于旧版本，屏障后的结果属于新版本。未变设备不停止采集，但屏障期间事件进入有界缓冲；提交后统一按活动映射标注，不能在同一批次混用版本。

修改解码、缩放或地址的点，其旧值不能作为新配置下的首值，提交后先置 UNKNOWN，等待新采样。仅名称变化可保留数值缓存。应用失败恢复旧任务/映射；提交之前候选数据不进入正式遥测队列。

## 7. 持久确认与恢复

QoS 1 的 PUBACK 是 MQTT 跳间确认，不等价于接收端数据库已提交；因此本方案自定义 data/ack=STORED 作为删除本地批次条件。这个应用层选择依据 [OASIS MQTT 3.1.1 第 4.3 节](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html) 的 QoS 确认边界。

状态流：CREATED → DURABLE → IN_FLIGHT → STORED → 删除。IN_FLIGHT 只表示尝试发送，不允许直接删除。断连或 ACK 超时返回可重试状态，保持 messageId、原始采样时刻和 payload 不变。

建议初版应用 ACK 超时 30s，退避上限 60s，最多 8 批在途；重试次数不作为静默丢弃依据，最终受队列容量和 7 天补传期限约束。永久拒绝进入有界死信，记录原因。

新数据和补传按 1:1 的批次配额轮转，任一侧空闲时另一侧使用全部配额；心跳和配置确认优先。限流还必须按字节数控制，不能只统计条数。

## 8. 停用语义

子设备 enabled=false：停止其采集及重连，保留配置和历史，状态 DISABLED。点位 enabled=false：从计划和周期报告移除，保留元数据，不伪造零值。

PLC 快照 config.enabled=false：停止所有子设备采集，Agent 的 MQTT、配置接收、心跳及已产生数据补传继续；这不是停止 Agent 进程。

停用、移除或点表变化后，历史队列仍按原 configVersion 补传，接收端使用对应历史快照解释。已经禁止的设备写操作不会因重放触发，因为首期协议无写命令。

## 9. 独立联调剧本

| 步骤 | 输入/操作 | 必须观察到的结果 |
|---|---|---|
| 1 | 启动 Agent，无业务配置 | WAITING_CONFIG，MQTT 可接收配置 |
| 2 | 发布 PLC-01 下 A/B 两子设备快照 v1 | APPLIED，A/B 分别开始采集 |
| 3 | A 原始寄存器=2206，scale=0.1 | A/Voltage=220.6，GOOD，采样时间保留 |
| 4 | 修改 A 周期并发布完整 v2 | A 新周期，B 任务不重启 |
| 5 | 发布非法点表 v3 | REJECTED，activeConfigVersion=2 |
| 6 | 断开 A | A 坏质量，B 正常；恢复后 A 自动采集 |
| 7 | 保持 Broker 正常，停止接收端 | PUBACK 可成功，但 Agent 队列不删除 |
| 8 | 恢复接收端并故意丢一个 data/ack | 重发去重，确认后删除 |
| 9 | 杀 Agent，Broker 不可用时重启 | 从已提交 v2 恢复 A/B 采集 |
| 10 | 发布 A 禁用、B 启用的 v4 | A DISABLED，B 正常，旧 A 数据可补传 |
| 11 | 队列容量缩小并断网 | 达上限后明确丢弃，磁盘不无限增长 |
| 12 | 两个不同 PLC 的 Agent 同时运行 | 配置、ACK、数据及 ACL 相互隔离 |

基础单元测试围绕错误代价高的逻辑：读块边界/字节序、配置事务崩溃点、ACK 去重、版本标注、报告死区及容量上限。性能和资源隔离在真机验证，不用单元测试代替。

## 10. 下一步产物顺序

先生成 contracts 中的配置/消息 Schema、有效与无效样例，再实现核心模型和持久化；随后实现 MQTT 与模拟端，形成配置闭环；接着实现 Modbus 及报告，最后完成故障恢复和真机验证。Hub 适配在这些结果稳定后开展。

当前未锁定 Python/库具体版本和镜像 CPU 架构，需依据首台 PLC 环境验证。协议模型与模块边界不依赖这些版本细节。
