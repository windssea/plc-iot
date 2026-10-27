# Python IoT子设备修改、采集启停与进程恢复

2026-10-27。实际Hub前端发布：v4将meter-001 current比例改0.2，真实数据24.8 A；v5停用该子设备采集，30秒内该版本目标GOOD=0，其余49台继续；v6恢复5点采集；另起进程run3使用原SQLite读取v6，重新Hub授时后恢复上传，无需重新发布；v7还原0.1，12.4 A。其他49台配置与报告配置不变，目标最终也逐项还原。

run2与run3均退出0、无failure、退出时Outbox排空、SQLite完整性正常。run3 boot为`boot-6e959a06da5a4cb1a161b0ef4427b852`，HUB_MQTT/SYNCED，TimeValid/ControlReady有效；首DATA晚于首Hub时间响应。被动窗口134个实际收据/7750样本/250点与开发库哈希和样本多重集匹配；这是捕获窗口，不是完整无损入口分母。

最终保留正常开发Agent运行：`python -m tools.agent`，launcher63276→Python65804同一进程树，activeConfigVersion7。正常入口最初按文件执行发生tools导入失败，已改用项目模块入口；失败进程已退出。启动配置和stdout/stderr在`.local/child-flow-20261027/normal-agent-*`，最终再次读取现有数据库证明持续采集。此进程未安装Windows服务或容器自启动，也未声称长期稳定性验收完成。

使用原有`.local/setup-preview/agent.db`、`telemetry.db`，既有库迁移到v2继续运行，未创建临时数据库。原始备份保留，**本轮不把旧空SQLite恢复覆盖当前v7及正常采集**。现有I06/T04持久Outbox保护复用，未额外开展新的提交点/断电组合专项。

完整双端结果、原PLC工程恢复及剩余主线见[三方记录](../../../PLCnext-Engineer/docs/三方主线-子设备点表启停与软件恢复验证-20261027.md)；[机器证据](../../../PLCnext-Hub-Backend/docs/verification/child-flow-20261027/evidence.json)。Hub保持唯一后端、现有开发数据库；P06继续后置。
