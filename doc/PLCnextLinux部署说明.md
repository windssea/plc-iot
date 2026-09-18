# PLCnext Linux 部署说明

本阶段提供源代码发布包、固定依赖集、安装/切换脚本、本地预检和 systemd unit。这是 0.1.0 起的文件配置入口（bootstrap.json / mqtt.json / agent.env）。**0.2.0 优先使用容器或 WBM App**，由浏览器初始化，不必在现场维护这些文件。裸机 unit 现已使用 `--driver all`，可同时执行 Modbus TCP 与 OPC UA。已验证 Linux x86_64 容器中的安装、普通服务用户运行、SIGTERM 退出和配置恢复。没有连接真实 PLC，也未在容器中运行 systemd 管理器。

## 目标环境

目标要求 Python **3.12**（含 venv、pip、SQLite、SSL）、可写持久目录，以及运行中的 systemd。预检会报告架构。不同 PLC 型号和固件的 Python、libc 与初始化系统不可默认相同：Phoenix Contact 的 [2025 固件应用变更说明](https://www.plcnext-community.net/robohelp/infocenter/assets/docs/ah_en_application-relevant_changes_fw_2025_111781_en_00.pdf) 包含 SysV 到 systemd 的迁移事项。旧固件需单独适配，当前脚本不安装 Python、不修改 PLC 原有运行时。

依赖版本见 `requirements-runtime.lock`，包括传递依赖。`rpds-py` 与 `cryptography` 含原生扩展，离线 wheelhouse 必须匹配目标 CPU、Python ABI 和 libc；不要将 Windows 虚拟环境拷到 PLC。当前锁文件锁版本，未提供供应链签名或依赖包哈希锁。预检核验 jsonschema、pymodbus、paho-mqtt 和 asyncua 的锁定版本。

## 发布布局

- `/opt/plcnext-iot/releases/<版本>`：只读代码和该版本专属虚拟环境。
- `/opt/plcnext-iot/current`：当前版本软链接。
- `/etc/plcnext-iot/`：PLC 身份、Broker 配置和凭据，升级不覆盖。
- `/var/lib/plcnext-iot/`：配置数据库、遥测 Outbox 和锁文件，升级/停用不删除。

生成发布包（开发机项目根目录）：

```powershell
./.venv/Scripts/python.exe -m tools.build_release --version 0.1.0 --output .local/releases/plcnext-iot-0.1.0.tar.gz
```

打包使用白名单，排除本地数据库、虚拟环境、测试配置和模拟器；输出文件存在时拒绝覆盖。包内 manifest 记录每个文件的 SHA-256，用于检测损坏，不能替代可信分发渠道。源文件统一 UTF-8/LF。

## 首次安装

先由管理员建立 `plcnext-iot` 普通服务账户和同名组，禁止交互登录。各固件用户管理工具不同，不在安装脚本中假定 useradd/adduser 参数。将包通过可信渠道传到 PLC，解压后执行（root）：

```sh
tar xzf plcnext-iot-0.1.0.tar.gz
cd plcnext-iot-0.1.0
sh deploy/linux/install.sh 0.1.0 /usr/bin/python3.12
```

将解释器路径改为目标机实际 Python 3.12 路径。安装只暂存版本、安装依赖并创建缺失的配置模板，不启用服务。安装中断时保留目录供检查，重试使用新发布 ID；不要在已有版本目录上覆盖安装。

离线安装前设置 `PIP_NO_INDEX=1`、`PIP_FIND_LINKS=/绝对路径/wheelhouse`。离线包需提前在匹配目标环境中准备、校验，并连同发布包传输。

编辑 `/etc/plcnext-iot/bootstrap.json` 的 gatewayId，必须与平台中 PLC ID 一致。mqtt.json 默认 TLS 和无效示例域名，改为实际 Broker、用户名；使用私有 CA 时加 `caFile`，建议绝对路径。创建 `/etc/plcnext-iot/agent.env`，内容为 `IOT_MQTT_PASSWORD=实际密码`，所有者 root，权限 0600；使用 systemd EnvironmentFile 的转义规则。不要将此文件放入源码或发布包。

在服务账户环境中提供同名密码环境变量，再运行预检（不要将密码写在命令行参数）：

```sh
cd /opt/plcnext-iot/releases/0.1.0
.venv/bin/python -m tools.preflight --bootstrap /etc/plcnext-iot/bootstrap.json --mqtt /etc/plcnext-iot/mqtt.json --require-systemd --release-root .
```

预检不加载 EnvironmentFile，命令行调用需自行设置环境；systemd 会在 ExecStartPre 前加载该文件。预检检查 Python、核心依赖、SQLite、配置、TLS CA、数据目录可写及至少 256 MiB 可用空间；仅用独立临时文件做写盘检查，不打开或迁移现有数据库，不连接 Broker。安装后以服务用户执行 `pip check` 可进一步验证依赖关系。

先验证 unit 与目标 systemd 的兼容性，再以 root 激活：

```sh
systemd-analyze verify /opt/plcnext-iot/releases/0.1.0/deploy/linux/plcnext-iot.service
sh /opt/plcnext-iot/releases/0.1.0/deploy/linux/activate.sh 0.1.0
systemctl status plcnext-iot.service
journalctl -u plcnext-iot.service -n 100 --no-pager
```

首次验证时 ExecStart 路径可能因 current 尚未建立而报告不存在；需区分该路径提示与不支持的 unit 配置项。激活会停止旧服务、切换软链接、安装 unit、启用开机启动并启动新进程。脚本返回成功只代表进程启动，验收仍需确认 MQTT 心跳、已提交配置版本和实际采样。

## 服务与日志

服务以专用用户运行，仅开放数据目录写权限。日志进入 journal，默认 `--quiet-samples` 关闭逐点诊断日志，遥测上传不受影响；排查时可手动启动并省略该参数。Broker 离线由 MQTT 重连逻辑处理，内部致命错误退出后由 systemd 重启。5 分钟最多启动 5 次，排除故障后用 `systemctl reset-failed plcnext-iot` 恢复。

SIGTERM 走配置协调器、采集器和 Outbox 的正常关闭路径。unit 的停止上限为 1800 秒，为最坏情况下多个子设备逐个关闭留余量；现场修改 operationTimeoutSeconds 或设备规模后应重新测量。超过上限 systemd 可强制终止，不能将超时退出视为完整刷盘成功。[systemd 官方服务定义](https://github.com/systemd/systemd/blob/main/man/systemd.service.xml) 说明了 Restart 和 TimeoutStopSec 的行为。

## 升级、回退和停用

升级先停止服务并备份整个 `/var/lib/plcnext-iot`（包含 SQLite WAL/SHM）、配置目录和当前版本记录，再恢复旧服务继续运行或进入维护窗口。新包用新 ID 暂存，先预检，再执行 activate.sh。停止失败时脚本不会切换代码。新版本启动失败时保留新目录和日志，不自动改写数据库或尝试降级。

代码回退使用同一 activate.sh 指定已安装旧版本，但必须先核对数据库格式兼容性。当前格式为 v1，不承诺未来版本可以直接降级。只有确需恢复备份且已明确接受期间新数据丢失时，才由管理员处理数据回退；脚本不自动恢复数据库。

停用并卸载服务注册（保留程序、配置和数据）：

```sh
systemctl disable --now plcnext-iot.service
rm /etc/systemd/system/plcnext-iot.service
systemctl daemon-reload
```

不提供一键删除持久数据。清理旧发布目录前确认它不是 current 指向的版本，并保留需要回退的版本。

## 自动验收与后续

```powershell
./.venv/Scripts/python.exe -m unittest discover -s tests -q
./.venv/Scripts/python.exe -m tools.deployment_smoke
```

Linux 验收使用独立 `python:3.12-alpine` 容器，在容器内执行安装及信号测试，不挂载主机系统配置/数据目录。生成的包与脚本留在 `.local/deployment-*` 供复查。systemd 开机启动、TLS 实际握手、PLC CPU/内存/闪存消耗及长期断网补传仍需在目标型号/固件上验收。0.2.0 现场安装优先走容器或 WBM；裸机脚本仍可用于无 AppManager 的环境。真机部署完成后再对接 Hub。
