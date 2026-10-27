# 固定工程入口与 Python 启动漏轮询修复

2026-10-27（Asia/Shanghai）。[工程入口](../../../PLCnext-Engineer/projects/README.md)已统一；本轮未连接、编译、下载或控制 PLC，P06 继续后置。Python 连接复用修复通过隔离及现有开发环境的真实启动/同进程重连验证。

## 工程与版本

稳定原工程归档为 `projects/stable/native-0.5.0/NativeGateway.pcwex`，SHA-256 `7c0724cdbe18552956b5f111e2c9ae0feb2829f9d28cfa2fd449ffe8bac46154`，用于恢复旧 RTC 兼容运行程序。授时候选归档为 `projects/candidates/native-0.7.0-timesync-candidate.1/NativeGateway.pcwex`，SHA-256 `b585662abf3d86a1d0445d59e9ed4354723edebafd1f0fcb4b81647a7fe80d41`，用于后续三方功能联调。均在 Engineer 仓库内；根工程已成为候选的精确副本，原根目录 P06 试验输入另存 experimental。

已验证归档不修改内嵌版本号：候选心跳仍为历史值 `native-0.7.0-p05p06`，必须用 artifactId/hash 区分，不能只看心跳版本。归档保留原始字节，不把重新打包或初始临时输入冒充已验证修订。工程清单验证三个文件、32/66/61 POU、内嵌标识及根别名；`build_native.py` 改为输出开发文件，防止覆盖已审阅入口。完整矩阵/P06未通过，当前没有最终授时发布版。

## 启动开销与修改

原实现为每个 device 建独立客户端；50个 unitId 共享 endpoint 时，端点预算将首次连接也串行化。当前锁定 PyModbus 3.13.1 在 connect 内有100ms固定等待，50次连接仅此部分的串行下界已为5000ms，等于轮询周期。本轮在自有127.0.0.1动态端口、真实TCP、同一50台250点/三读块配置下复现，未修改第三方库或访问现场设备。

| 12秒隔离窗口 | 修改前 | 修改后 |
|---|---:|---:|
| TCP连接/实际connect调用 | 50 / 50 | 1 / 1 |
| connect累计耗时 | 5596ms | 109ms |
| 漏轮询/按物理槽独立数缺口 | 161 / 161 | 0 / 0 |
| 物理结果 | 589 | 750 |
| 全部点最终GOOD | 250 | 250 |
| 停止后连接 / 入口溢出 | 0 / 0 | 0 / 0 |

隔离环境复现161条，历史真实环境为314条；不宣称两次启动开销逐项相等或已唯一解释历史每条缺口。修复消除了已测得的重复连接串行开销，真实复验见下一节。未延迟首次激活原点、丢弃前两槽、改5000ms周期、提高全局并发或清零漏槽计数。

生产改动限于 `drivers/modbus.py`、`devices/modbus.py`、`devices/runtime.py`：每个 DeviceRuntime 拥有最多50个 endpoint 的引用计数连接池，以小写host/port归一化。各 unitId 的请求字段、connect/request超时及重试仍按各自配置；端点读串行与全局16预算不变。SDK内部超时覆盖契约最大值，由每次调用外部有界超时控制，避免短超时首个所有者影响其他unit。最后读者退出即关闭/删除entry；请求超时或取消在释放锁前关闭旧socket，后续unit重新连接；取消锁等待者不关闭正在工作的其他unit。协议异常保留连接；配置切换、回退与shutdown处理各自引用，不共享跨Runtime全局缓存。

9项新增真实loopback专项覆盖unitId/地址串行隔离、退出引用、独立超时、迟到响应、两类取消、协议恢复、端点/Runtime隔离、容量、配置回退及shutdown。完整回归376项通过（44.601秒）。首轮有2失败/4错误，修正的是此前授时/事件约束落地后未更新的旧夹具：缺runtime、非GOOD带数值、非法GOOD构造、防御性展示、任意墙钟年龄过期，以及用刚启动诊断缓存断言持久库内容；未放宽生产规则。保留首轮和中间失败日志。

## 数据库约束执行偏差与修正

全量discover中的旧测试创建并清理了自有临时SQLite夹具，这不符合本轮沿用的“不创建临时数据库”约束，是执行偏差；不是另建开发PostgreSQL或把备份作为运行库。失败/成功日志均保留，不以376项通过掩盖该偏差。临时夹具由测试清理，真实联调仍仅使用既有数据库并已恢复。

已新增 `python tools/run_startup_regression.py` 作为后续本轮回归入口：在整个测试期间将 `sqlite3.connect` 替换为立即失败的守卫，55项采集/连接池/预算/时钟/内存Outbox/MQTT组合测试全部通过（3.983秒），没有SQLite连接。后续本轮不再使用会创建临时库的全量discover；真实联调仍使用已有库的一致备份/恢复流程。

## 真实启动、重连及独立对账

唯一Hub后端27204/8080、前端35404/8090与mock23716保持。使用既有开发PostgreSQL及既有空闲 `.local/setup-preview/{agent,telemetry}.db`；按此前流程一致备份、显式v2迁移，只运行这个受控Python进程，结束后恢复原v1逻辑内容。未新建联调数据库，备份没有作为运行库。全量单元测试沿用各自拥有并清理的临时夹具，与联调数据库分开记录。

原始入口审计和真实旁路先启动，再运行生产Agent/Modbus采集/逻辑时钟/持久Outbox。测试仅重连自有MQTT客户端，不停止Hub/broker/mock，不发送伪DATA/STORED，不改操作系统日期；注入的错误系统日期仍为2000-01-01，仅逻辑时钟使用Hub授时。

- boot `boot-0407810aa7ea4d84915c27c6ee496a94`，配置1，50台250点，两个session。
- 完整入口2250条：250个首次UNKNOWN质量事件、2000个物理结果；独立选择2250条，全部真实STORED及本地释放，S=2250、P/L/I/U=0。
- `missedPollCyclesTotal=0`、`missedReportSlotsTotal=0`；250点GOOD，无入口溢出、未决丢失、入队拒绝、隔离或不确定事件，尾部排空，Outbox pending/dead均0。
- 42个唯一批次的收据hash及2250条原始入库行多重集全部匹配。旁路有43次DATA投递（含1次原字节重试），不重复计采样或入库；8个授时请求、6个精确回显响应。
- 独立入口核验器验证两个session各自先授时/heartbeat/config-get控制，再提交DATA。较粗的`dataAfterObservedTimeResponses=false`是在把首条DATA与**整轮最后**授时响应比较，重连场景正常出现；不能用该字段代替逐session检查。独立DB工具只做SQL只读查询，业务写入仍由真实Hub完成。

新Python物理结果与既有PLC boot71/M13原始证据比较：1532个共同物理槽，值/质量/选择精确binary64匹配1532、差异0，槽0～5各250点、槽6为32点；双方共同观察范围漏槽均0，Python窗口外尾部468条单列。450个时间资格差异来自独立启动授时阶段，不比较跨boot UTC，不把单独运行当动态输入锁步重放。本轮没有重新跑PLC。

此前314漏轮询/2报告漏槽及旧运行340 TIME_INVALID、全局VOLATILE_RESTART UNKNOWN原样保留；本轮只证明新受控窗口已无同类漏槽，并非全局历史无损、正常30分钟或所有失败重连/慢网关场景已验。PLC逐事件决策时钟/独立漏槽事实和剩余共同矩阵仍待验。

最终agent/telemetry均恢复原v1、quick_check=ok、原身份/空配置/空Outbox逻辑内容一致。观察器及受控Agent全部退出；mock50台运行、250 Good、config_error=null。PLC最后已验证状态仍为原native-0.5.0/boot72，本轮没有新增设备状态观察。未提交Git。

机器结果和原始文件hash见[证据清单](2026-10-27-project-entry-python-startup-evidence.json)。入口漏槽独立分析的历史结论见[前置分析](2026-10-27-joint-slots-startup.md)。
