# 本地初始化与 PLCnext App 部署方向

## 当前已经实现

0.2.0 容器默认运行 `tools.appliance`。首次启动打开本地初始化页面，填写已有 PLC ID、Broker 地址/端口、TLS、用户名/密码和可选私有 CA 证书。支持独立测试 Broker 认证，也支持离线保存后自动重连。无需上传或编辑 bootstrap.json、mqtt.json、agent.env。初始化完成后页面显示当前配置，并允许修改平台连接部分。

```sh
docker load -i plcnext-iot-amd64.tar
docker compose -f deploy/container/compose.yaml up -d
```

然后访问 `http://设备IP:8080`。ARM64/ARMv7 使用对应标签，通过 IOT_IMAGE 选择镜像；Compose 默认标签为 `plcnext-iot:0.2.0-amd64`。端口 8080 如与其他应用冲突，需要由部署方调整宿主机映射。

页面及全部静态资源随镜像提供，不访问外部 CDN。测试连接只验证 Broker TCP/TLS 和 MQTT 认证，不验证 config/set 等 Topic 权限。保存成功只表示本地写入成功，页面不会将它显示为已联网。

## 保存和恢复

初始化记录保存在持久卷 `/var/lib/plcnext-iot/commissioning.json`，采用同目录临时文件、fsync、原子替换；Linux 文件权限 0600。密码是受文件权限保护的明文，不宣称设备端加密。私有 CA 从该记录恢复为运行时 PEM 文件。初始化进程全程持有独占锁，阻止两个初始化实例同时拥有目录。

保存后自动运行既有采集 Agent，驱动为 `all`（Modbus TCP 与 OPC UA）。重启直接加载初始化记录，沿用 agent.db 和 telemetry.db。配置损坏时退出报错，不清空或重新认领；发现旧数据库但没有初始化记录时拒绝首次设置，防止给旧采集数据绑定新 PLC 身份。页面只配置北向 MQTT；OPC UA 用户名在业务快照中，对应 `passwordEnv` 需由容器环境另行提供。

初始化完成后页面不是通用管理后台，而是一个有边界的维护入口：可查看已保存的 PLC ID 与 Broker 设置，并可修改 Broker 地址、端口、TLS、用户名/密码和私有 CA。**PLC ID 不可修改**：它与设备上的 agent.db、telemetry.db 身份绑定，改动会让采集子系统启动失败，换身份属于单独的数据迁移流程。页面永不返回密码与 CA 内容，只报告"已设置/未设置"；修改前需要一次页面内二次确认。当前仍没有远程重置按钮；不要删除 commissioning.json 后保留数据库并尝试重新初始化。

修改后的生效方式：进程与初始化端口不重启，采集子系统在进程内重新加载（重新物化生效值、重建 MQTT 与采集运行时）。因此注意三点：

- 采集与平台上报会短暂中断，通常数秒；遥测 Outbox 中未确认的批次继续补传，由接收端按 messageId 去重。
- 同一进程内重载保持 bootId 不变，接收端不会把它误判为 PLC 重启，uptimeSeconds 与 sequence 继续累计。
- 接收端每次重载后各采集点基线清空，会出现一轮全量上报；跨 Broker 切换时 telemetry.db 复用，旧批次会被发往新 Broker（若新 Broker 属于另一套接收端，可能长期重试直到过期）。

## 访问边界

首次设置尚无管理账户，采用可信现场管理网络的首次认领模式。知道设备地址且能访问初始化端口的人可首次设置，因此不应将未初始化的端口暴露到公共或非可信网络。

**已接受的风险：**开放修改后，这个边界从"一次性认领"变成"持续可写"。页面令牌只防跨站请求伪造，任何能访问 8080 的人都能读到它，所以任何能访问该端口的人都能永久改写 Broker 地址并收走全部遥测。密码不可读出但可被覆盖。因此初始化端口必须始终限制在可信管理网络内，这与首次设置的前提相同，但不再是短暂窗口。HTTP 无传输加密；正式现场可由受控 HTTPS 入口转发，或在 appliance 上使用 `--cert`、`--key` 启用 HTTPS。页面提示该边界，不能因为 Broker 使用 TLS 就认为浏览器到设备也是加密的。

HTTP 服务限制并发连接数、请求头/体大小和请求时间；修改请求要求页面令牌和同源检查，禁止 CORS。身份写入在首次设置后锁定，Broker 设置可继续修改。页面禁止 iframe 嵌入，未来从 PLCnext WBM 使用独立链接打开，不能直接假设继承 WBM 登录身份。

本地开发预览默认仅监听回环：

```powershell
./.venv/Scripts/python.exe -m tools.appliance --data-directory .local/setup-demo --port 8080
```

容器内默认监听 0.0.0.0，以便映射宿主机端口。原有 tools.agent 命令仍保留；旧文件部署可显式以 `python -m tools.agent` 作为入口运行。裸机 systemd unit 仍是旧文件模式，新的免文件部署优先使用容器入口。

## PLCnext 自带界面安装的最终方案

方向确定为 **PLCnext Function Extension，包含 OCI container App-part**，由 PLCnext AppManager 安装/启动，底层使用 Podman 与 systemd。官方资料给出的 OCI App-part 最低固件为 2025.0.x；旧固件不能直接套用。参考：

- [PLCnext 官方 App 示例及 OCI 元数据说明](https://github.com/PLCnext/PLCnextAppExamples)
- [Function Extension 构建指南](https://store.plcnext.help/st/Creating_a_Function_Extension/Building_a_PLCnext_Control_Function_Extension.htm)

最终交付结构应包含正式 App 标识、目标型号和最低固件声明、匹配架构的 OCI 镜像、Quadlet 启动定义及 App 持久目录声明。App 更新必须保留持久目录，容器内映射到 `/var/lib/plcnext-iot`，并验证 rootless UID 映射下的写权限。安装后打开初始化页，完成一次设置即可开始工作。

当前已经完成容器内页面和自动恢复，并生成 AXC F 2152 的 SquashFS `.app`。该包已在 2152 真机安装并启动。vPLC 本身是宿主机上的容器，把 OCI `.app` 再装进 vPLC WBM 会嵌套 Podman，启动时报 `OciContainerPartError`。vPLC 侧在宿主机加载 AMD64 镜像并用 Compose 运行，见 [多架构容器镜像说明](多架构容器镜像说明.md)。下载、2152 重建和验收步骤见 [WBM 安装包说明](WBM安装包说明.md)。

## 验证

```powershell
./.venv/Scripts/python.exe -m unittest discover -s tests -q
./.venv/Scripts/python.exe -m tools.provisioning_smoke
```

覆盖首次保存、重启读取、并发首次保存仅一次成功、跨站请求拒绝、非法输入、私有 CA 的写入/保留/移除、TLS 关闭时清空 CA、敏感字段不回显、历史记录兼容读取、PLC ID 拒绝修改、生效值未变不触发重载、旧库/损坏记录保护、慢连接关闭及真实 MQTT 自动启动。`tools.provisioning_smoke` 额外在两个真实 Broker 上验证：改 Broker 后新 Broker 收到 config/get，且初始化进程未退出。浏览器实际检查了表单、TLS 默认端口联动、连接失败提示、配置摘要、修改前二次确认和离线保存完成页。ARM 镜像可构建，ARM 实际运行仍需对应设备或仿真环境验收。

## 已确定的首批目标（2026-09-09）

用户指定 AXC F 2152 和 vPLCnext，并希望匹配最新固件。发布策略以官方当前稳定/LTS 分支的最新补丁为准，在制作 App 安装包时固定完整版本；不使用长期漂移的 latest 作为验收版本。

| 目标 | 镜像架构 | 固件适配方向 |
|---|---|---|
| AXC F 2152 | linux/arm/v7 | 2026.0 LTS 系列，发布时核对官网下载的最新补丁 |
| VPLCNEXT CONTROL（先按官方 x86-64 镜像） | linux/amd64 | 2026.0 系列，核对具体虚拟控制器产品和镜像版本 |

AXC F 2152 的 Cortex-A9 是 32 位 ARM，本项目应使用 armv7 包。vPLC 官方仓库示例为 x86-64；不能用该示例中的 2025.0.0 标签断言当前最新版。官方 2026 年公告已同时列出 AXC F 2152 和 VPLCNEXT CONTROL 的 2026.0.3 固件，但该公告并非完整版本目录，本次未独立确认两款产品的最新补丁号。

资料：[AXC F 2152 官方产品页](https://www.phoenixcontact.com/en-us/products/controller-axc-f-2152-2404267)、[vPLCnext 官方部署仓库](https://github.com/PLCnext/vplcnextcontrol)、[官方 2026.0.3 版本相关公告](https://assets.phoenixcontact.com/file/a9721fd9-1ad4-495c-b341-15d3a5f363a9/media/original?pcsa-2026-00005_vde-2026-050.pdf=)。

vPLC 运行在宿主机容器内。IoT 采集器与它并列部署在同一台宿主机，不进入 vPLC 的 AppManager。2152 仍走 WBM Function Extension。页面和业务逻辑共用，安装物分开。
