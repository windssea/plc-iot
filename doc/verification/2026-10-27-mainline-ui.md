# Python IoT真实Hub页面闭环

先提交主线整理 `8b2cc6c`，再运行真实Agent，连接现有Hub/MQTT/Modbus。已有 `.local/setup-preview/agent.db`、`telemetry.db`用于本轮，未创建临时数据库。使用 `tools/verify_platform_live.py prepare`保留一致备份并显式迁移已有Outbox v1→v2；备份不是新的运行数据库。

- Hub页面保存、预览、发布报告间隔1100ms为v2，再恢复1000ms为v3；真实ACK和Hub发布记录均APPLIED，Agent版本1→2→3。
- 启动由SYNCING到HUB_MQTT/SYNCED，time_valid/control_ready=true；首DATA晚于同boot首授时响应。50台/250点真实Modbus数据，Hub页面新版本实时值与历史可见。
- 被动窗口150收据/9250条样本与现有PG的hash/多重集匹配，覆盖250点；不是全源队列无损或性能验收。
- 新辅助工具 `tools/verify_mainline_live.py`只观察正常Agent，本轮未启用AuditRecorder、假ACK/数据或系统改时。仅允许两个明确且已存在的SQLite路径。

首次停止协调使用缓存pending=0，正常关闭仍保存最后4批。没有清库或覆盖这些批次；保持原记录，通过后续正常Agent启动、自动授时和真实Hub ACK排空。辅助工具已改成存储新鲜计数、入口/选择/工作线程持续空闲2秒及关闭后批次检查，避免把缓存0当作恢复备份许可。

后续20秒正常运行得到active3和HUB_MQTT/SYNCED，退出后Outbox空、两库integrity=ok；随后才通过原工具restore恢复原有开发SQLite内容，commissioning未改。未把这个辅助脚本问题说成产品丢数，也不把一次正常进程重开扩展成完整宿主/断电恢复已通过。

原始记录 `.local/mainline-ui-20261027/python-run.json`、`python-drain-run.json`、manifest与restore文件保留。机器摘要与前端截图见[Hub记录](../../../PLCnext-Hub-Backend/docs/verification/2026-10-27-mainline-ui.md)，最终PLC原工程恢复见[三方本轮记录](../../../PLCnext-Engineer/docs/三方主线-Hub真实前后端闭环验证-20261027.md)。后续仍先完成正常子设备配置、断连/软件恢复、实际宿主自启动和持续运行，P06后置。
