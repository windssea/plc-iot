# 共同物理槽比较与Python启动漏轮询

> 本文是修复前分析。后续连接复用及真实零漏槽复验已完成，见[修复报告](2026-10-27-project-entry-python-startup.md)；旧314记录保留。

2026-10-27。只分析既有真实运行和本地源码；本轮没有重跑Python Agent、迁移SQLite、改业务代码、运行服务或操作PLC。共同证据见[三方报告](../../../PLCnext-Engineer/docs/三方联调-M13有限窗口闭合与逐槽对标-20261027.md)。

## 已证实的比较

Python boot-88420ac8e87a43ec89d2e41bfcdca892的2186个物理结果，与PLC boot71有限源窗口的1532个物理结果按不可变配置、endpoint/unitId/点名和首次激活原点对应。1218个共同槽的binary64工程值、质量和选择完全一致，槽2～5各覆盖250点；未用epsilon、显示精度或UTC相等作为对标依据。

Python在共同范围缺314个物理结果（槽0缺156，槽1缺158），恰与原运行missedPollCyclesTotal=314一致；energy/power/voltage各100，current/frequency各7。它们是源层未创建结果的轮询槽，不应混入policyFiltered、TIME_INVALID或已捕获cohort的S/P/L/I/U。原运行missedReportSlots=2仍是另外的计数。

报告原点会随逻辑时间代际重新锚定，直接使用captured.nominalOriginMonoMs重编号会造成90个假重复/覆盖。新工具以第一次QUALITY_STATE UNKNOWN的completedMonoMs为采集原点，完整保留2186个物理结果；真实同点同物理槽重复仍明确拒绝。对当前冻结配置有效，不扩大为所有重配置/启停场景已验。

## 源码层启动开销候选

- devices/modbus.py在endpoint预算内调用reader.read_block；首次连接也在此范围内。
- devices/budget.py以(host,port)串行，等待者不占全局槽；全局上限16保持。
- drivers/modbus.py每device各有ModbusReader/客户端，首次读取创建AsyncModbusTcpClient并connect。
- 本环境PyModbus client/base.py的async connect在网络connect后固定await asyncio.sleep(0.1)。本profile50个unitId共享同一endpoint，50次首次连接仅固定等待的串行下界就为5000ms，已等于poll周期，尚未计网络/调度/解码。

已证实存在这一开销，尚未为314个漏槽分别记录完整等待链。不得直接断言全部由该项、授时往返或mock故障导致；本轮mock正常也不证明历史自动退出的死因。

## 后续收敛方式

优先在自有loopback做50unit/三读块的启动取证，再实现有界共享端点连接或等价有界方案。保持全局16与端点读串行，不通过延迟激活原点、丢掉前两槽、改poll周期或调低标准得到零漏槽。

连接共享须验证unitId/每请求地址与超时隔离、取消/超时后的socket代际、引用计数、配置替换/回退与关闭、其他endpoint公平、失败重连不复活旧任务。不要直接删除第三方100ms等待或盲增并发。

隔离验证通过后，再使用既有开发库/唯一后端及既有SQLite一致备份恢复流程做一次真实Python入口对账；PLC保持原运行工程。P06性能优化继续后置。当前尚未实施生产修复或宣布正常零漏槽通过。
