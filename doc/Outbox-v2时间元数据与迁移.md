# Outbox v2 时间元数据与迁移

2026-10-26，T04实施边界。仅升级telemetry.db（application_id=0x504F5554），agent.db及Hub开发PostgreSQL不变。旧v1程序遇到v2按原规则拒绝，不提供自动降级、删除重建或默认迁移。

v2在batches保留原payload/hash/message_id/created/attempts/next_due/state/reason，新增local_order、enqueued_utc_ms、uncertainty_ms、time_trust、time_basis_ref、enqueue_boot_id、clock_revision、terminal_source。time_trust为HUB_TRUSTED、LEGACY_UNVERIFIED、SYSTEM_UNVERIFIED；只有HUB_TRUSTED且与当前有效时间基准可比时，允许保守年龄下界达七天后过期。旧created仅供原值核对，永不回填或升格可信；next_due不参与新运行期单调调度。

local_order迁移自旧rowid，此后由meta.next_order单调分配，与INSERT同事务更新；删除最高序号不复用。meta另保留trusted_basis_ref/trusted_utc_high_water_ms（已知可信UTC下界），启动时先加载到逻辑时钟，大幅回拨或更换权威不凭新boot自动越过维护边界。terminal_source区分AGE_EXPIRED、PERMANENT_REJECT及旧记录未知来源，支持仍有原哈希/发送关联的迟到STORED；不能推导旧死信必为本地过期。

通过migrate_existing显式升级：要求既有、正确身份/格式/完整性的v1文件和一个全新持久备份路径；持有同一StoreLock，使用SQLite一致备份包含已提交WAL，核验并保存SHA-256清单，再以一个事务增加字段、索引和user_version=2。中断只留下完整v1或完整v2，原字节及计数不变；备份是恢复产物，不用作新建的测试数据库。回退需停写、明确恢复备份及处理升级后的新数据，不能用恢复旧库静默抹去新记录。

StoreWorker排队任务携带明确TimeToken，执行和入队/预留COMMIT决策前复核boot/时钟/连接修订及单调上下文期限。慢SQL在存储线程执行；事件循环不持有跨SQL/fsync的时钟锁。成功接纳线性化于COMMIT决策检查，此后时间失效不撤销该历史所有权；COMMIT失败仍由上游持有。入队UTC/误差在该决策处刷新，不以排队前快照计算年龄。ACK删除不依赖当前UTC，不触发过期扫描，哈希损坏不删除。

本次隔离策略测试使用注入仓储，不新建SQLite测试库或其他临时数据库。真实迁移验证只可使用已核对为空闲、原有的`.local/setup-preview/telemetry.db`，先一致备份，验证回滚/迁移/恢复后还原原v1逻辑内容；备份是持久恢复产物，不作为运行测试库。不启动Agent或接触设备，不把软件事务测试写成物理断电证明。

上述实际迁移及恢复验证已执行通过，原库仍为v1/零批次、agent.db未变；结果见[验证报告](verification/2026-10-26-t03-t04.md)。显式运维命令为 `python tools/migrate_outbox_time.py --database <既有telemetry.db> --gateway <原身份> --backup <全新持久备份路径>`。迁移前停止该Agent，先核验身份、完整性、原批字节/哈希、attempts和计数；失败保留原库，不删库重建。不能把本次恢复空样例库的步骤用于覆盖已经接纳新数据的真实v2库。

平台模式由MQTT设置的time对象显式选择，必须配置稳定timeBasisRef及有依据的maxDriftPpm/driftEvidence；测试夹具漂移数字只证明算法，不证明设备漂移已测。旧模式默认SYSTEM_RTC，明确未验证；其入队年龄也不自动变为可信。逻辑时间不写操作系统时钟。
