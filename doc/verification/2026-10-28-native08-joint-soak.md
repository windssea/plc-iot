# 与原生0.8的共同持续采集观察

2026-10-28。Python IoT业务代码和配置未改，正常开发入口61992→20160同一进程树，agentVersion0.2.0、配置7、boot-5dbd5f1ffb344504b34b259fb6e80785。使用既有setup-preview的agent.db/telemetry.db，不创建临时数据库，不回灌旧备份。

与PLC native-0.8.0/boot84在15:17:58.243914起共同观察1800秒：IoT实收1498批/90000个GOOD值，完整采样窗口入库90000GOOD；每分钟50台/250点，非GOOD0、错误ACK0、丢弃增量0，boot/会话/配置不变。队列峰值4批，末快照4批，采集继续，不声称排空。现有两库quick_check=ok，telemetry schema2。进程RSS约23.14→26.43MiB，峰27.01MiB；不冒充24小时或目标宿主自启动验收。

PLC已完成同一0.8工程的下载、Hub自动授时、普通断连与整机软件重启恢复及上述观察，0.5仅回退。[完整三方报告](../../../PLCnext-Engineer/docs/主线0.8发布与同版本验证-20261028.md)，[共同观察证据](../../../PLCnext-Hub-Backend/docs/verification/native08-mainline-20261028/result.json)。后端50864保持一个实例，P06后置。
