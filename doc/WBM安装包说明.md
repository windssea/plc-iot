# WBM 安装包（0.2.0）

已生成两份 SquashFS `.app`，位于项目 `.local/wbm/0.2.0/`。镜像内含离线初始化页面，无需手动上传配置文件。

| 文件 | 目标 | 架构 | 固定本地开发 App ID |
|---|---|---|---|
| plcnext-iot-0.2.0-axcf2152-unsigned.app | AXC F 2152 | linux/arm/v7 | 90260909002152 |
| plcnext-iot-0.2.0-vplc-unsigned.app | VPLCNEXT CONTROL 500、1000、2000、3000 | linux/amd64 | 90260909001000 |

两份包最低固件声明均为 **2026.0.3**；这不是对最新补丁版本的断言。vPLC 目标字符串采用上述官方产品名称，尚未与实际设备的 WBM Information > Type 逐一比对。

## 开发包状态

用户暂无 PLCnext Store 分配的正式 App ID，因此使用固定本地开发标识。两份包均**未签名，未经过 WBM 真机安装验证**，不是已获 Store 认可的发布包。2026.0.3 官方安全公告包含 App 真实性校验修复；实际安装是否接受此开发包取决于目标固件的校验策略。若被拒绝，需要按官方支持的可信发布流程取得正式标识并完成真实性验证，不应降级固件或关闭安全检查。

已验证输入镜像架构、原生扩展 ELF 架构、初始化入口、官方 App Info Schema、SquashFS 格式，并从实际 `.app` 回读元数据。AMD64 初始化容器此前已运行验证；ARMv7 尚未在设备上运行。结构检查不能替代 AppManager、Podman 和设备权限验收。

## 设备安装与初始化

1. 在 WBM 核对固件、Information > Type 和设备架构，选择对应包。
2. 在 WBM 的 PLCnext Apps / AppManager 页面上传 `.app`，查看安装结果。若出现目标型号或真实性校验错误，保留完整错误信息以便调整正式包。
3. 安装成功后启动 App，通过其 Initialization 端口链接或 `http://设备IP:8080` 打开初始化页面。
4. 填写 PLC ID、MQTT Broker 和凭据，测试连接或离线保存。保存后自动启动采集 Agent；重启后自动读取已保存记录。

8080 是独立 HTTP 入口，不继承 WBM 登录。首次初始化应在可信管理网络完成。若 vPLC 使用宿主机 NAT 端口映射，还需将宿主机可访问的端口转发到 vPLC 的 8080；仅转发 WBM 的 443 不会转发初始化页面。端口冲突可在重建时使用 `--web-port` 调整。

数据映射为 `${APP_PERSISTENT_DIR}/iot:/var/lib/plcnext-iot`，通过 rootless keep-id 将固件用户映射至容器 UID/GID 10001。包声明更新时保留持久数据。设备验收应覆盖首次保存、重启恢复、启停、同一 App ID 升级保留配置与数据库，以及卸载时的数据处理。首次保存后，页面可查看当前配置并修改 Broker 地址、端口、TLS、凭据与私有 CA；PLC ID 不可修改。修改会在进程内重启采集子系统，容器与初始化端口不受影响。

## 重建

需要 Docker 和项目 Python 环境，以及 `.local/images/0.2.0/` 中的 AMD64、ARMv7 镜像归档。在项目根目录运行，输出目录必须尚不存在：

```powershell
./.venv/Scripts/python.exe -m tools.build_plcnext_apps --output .local/wbm/0.2.0-rebuild
```

可用 `--axcf2152-app-id`、`--vplc-app-id` 指定正式的 14 位 ID；提供 ID 并不会自动签名。可用 `--vplc-targets "WBM 中的精确 Type"` 修正 vPLC 型号匹配；多个型号以逗号分隔。`--min-firmware` 控制最低固件声明，必须与实际验证范围一致。

同一设备后续升级应保留 App ID。开发 ID 更换为正式 ID 会改变 App 的持久目录归属，需要安排数据迁移，不能假设配置自动继承。

构建脚本同时生成 `manifest.json` 和 `SHA256SUMS`，记录架构、ID、最低固件、是否签名、文件大小及校验和。`source-*` 目录保留包内元数据、Quadlet 和镜像归档，便于审查。

## 打包依据

- [官方 OCI App-part 规范](https://store.plcnext.help/st/PLCnext_App_Integration_Guide/Apps_parts/OCI_container.htm)：离线镜像、Quadlet、rootless 和 App 持久目录变量。
- [官方元数据规范](https://store.plcnext.help/st/PLCnext_App_Integration_Guide/Apps_parts/Metadata.htm)：目标型号、App ID 和 WBM PortLink。
- [官方 App Info Schema](https://github.com/PLCnext/App-Info-Schema)：2026-09-09 下载并随项目保存于 `deploy/plcnext-app/app-info-schema.json`。
- [官方 SquashFS 打包示例](https://github.com/PLCnext/PLCnext-ROS-bridge)：使用 mksquashfs 和包文件 UID/GID 1001/1002。
- [官方 vPLC 仓库](https://github.com/PLCnext/vplcnextcontrol)。
- [2026.0.3 安全公告](https://assets.phoenixcontact.com/file/a9721fd9-1ad4-495c-b341-15d3a5f363a9/media/original?pcsa-2026-00005_vde-2026-050.pdf=)。
