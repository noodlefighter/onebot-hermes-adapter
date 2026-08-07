# OneBot v11 Adapter for Hermes Agent

一个 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 的 OneBot v11 平台适配器插件，让 Hermes 能够接入任何兼容 OneBot v11 协议的聊天服务器（如 NapCat、go-cqhttp、Lagrange 等）。

## 重要声明

**本仓库 100% 由 AI (小叽) 维护，零人工手写代码。**

所有代码、文档、提交记录均由 AI 生成。

## 免责声明

### 安全风险

- 本代码未经专业安全审计，可能存在未知漏洞
- 请勿在生产环境或存储敏感数据的系统中直接使用
- 使用前请自行评估安全风险
- WebSocket 连接建议使用 wss (TLS) 而非 ws 明文传输
- Access Token 应妥善保管，切勿泄露

### 责任限制

- 本项目按"现状"提供，不作任何明示或暗示的保证
- 作者不对因使用本代码造成的任何直接或间接损失负责
- 包括但不限于：数据丢失、服务中断、安全漏洞、账号封禁等
- 使用本代码即表示您理解并接受上述风险

### 合规提醒

- 请遵守您所在地区的法律法规
- 请遵守 QQ / OneBot 服务的使用条款
- 请勿用于发送垃圾信息、骚扰他人或任何违法违规用途
- AI 生成的代码可能包含意外行为，请在部署前仔细审查

## 功能特性

- 通过 WebSocket 连接 OneBot v11 服务器
- 支持私聊和群聊消息收发
- 支持图片发送（HTTP URL、本地文件、base64 编码）
- 用户白名单 / 全放行权限控制
- 群白名单过滤（`group_allowed_chats`）
- 群内仅响应 @ 机器人消息（`at_mention_only`）
- 未授权私聊静默忽略（`silent_unauthorized_dm`）
- 连接成功通知（`connect_notify`）
- Cron 定时任务投递支持（含图片附件）

### 图片发送支持

适配器实现了完整的图片发送能力，遵循 OneBot v11 协议的 `image` 消息段规范。

**支持的图片来源：**

| 来源 | 格式 | 说明 |
|------|------|------|
| HTTP/HTTPS URL | `http://example.com/photo.jpg` | 框架自动下载并上传 |
| 本地文件路径 | `/path/to/image.png` | 自动读取并转为 base64 编码 |
| `file://` URI | `file:///path/to/image.png` | RFC 8089 格式，自动解码 |
| Base64 data URI | `data:image/png;base64,...` | 自动转换为 `base64://` 协议 |
| `base64://` | `base64://iVBORw0KGgo...` | 直接传递给 OneBot |

**关键实现：**

- `send_image()` — 发送图片，自动解析各种来源格式
- `send_image_file()` — 发送本地图片文件（Gateway 媒体投递路径）
- `_standalone_send()` — 支持 `media_files` 参数（Cron 独立进程发送）

**工作原理：**

由于 OneBot v11 服务器（如 NapCat）通常运行在 Docker 容器中，无法直接访问宿主机本地文件路径。适配器会将本地文件读取后编码为 `base64://` 格式，嵌入到 OneBot 的 `image` 消息段中发送，确保容器内外文件系统隔离不影响图片投递。

## 安装

### 前置要求

- Hermes Agent (v0.14.0+)
- Python 3.11+
- `websockets` 库

### 安装依赖

**必须安装到 Hermes 的 venv 环境中**，否则插件加载时找不到依赖：

```bash
~/.hermes/hermes-agent/venv/bin/pip install websockets
```

验证安装成功：

```bash
~/.hermes/hermes-agent/venv/bin/python -c "import websockets; print(websockets.__version__)"
```

### 安装插件

将本仓库克隆到 `~/.hermes/plugins/` 目录下（非入侵式，不会被 `hermes update` 覆盖）：

```bash
cd ~/.hermes/plugins
git clone https://github.com/noodlefighter/onebot-hermes-adapter.git
```

### 启用插件

```bash
hermes plugins enable onebot11-platform
```

启用后重启 gateway 生效：

```bash
hermes gateway restart
```

## 配置

### 方式一：环境变量（推荐）

在 `~/.hermes/.env` 中添加：

```env
ONEBOT11_WS_URL=ws://your-server:6097
ONEBOT11_ACCESS_TOKEN=your-token
ONEBOT11_ALLOWED_USERS=123456789
ONEBOT11_ALLOW_ALL_USERS=false
```

### 方式二：config.yaml

在 `~/.hermes/config.yaml` 中添加：

```yaml
platforms:
  onebot11:
    enabled: true
    extra:
      ws_url: "ws://your-server:6097"
      access_token: "your-token"
      allowed_users:
        - "123456789"
      group_allowed_chats:
        - "540827889"
      at_mention_only: true
      allow_all_users: false
      silent_unauthorized_dm: true
      connect_notify:
        - "402156474"
      # 单条 record 语音在 Gateway 媒体缓存中的最大大小（20 MiB）。
      voice_media_max_bytes: 20971520
      # 将 NapCat 容器内 get_record 返回的路径映射到 Gateway 主机路径。
      record_path_map:
        - "/app/.config/QQ=/srv/napcat-data"
```

### 配置说明

| 环境变量 | config.yaml | 说明 |
|---------|-------------|------|
| `ONEBOT11_WS_URL` | `ws_url` | WebSocket 服务器地址 |
| `ONEBOT11_ACCESS_TOKEN` | `access_token` | 访问令牌（可选） |
| `ONEBOT11_ALLOWED_USERS` | `allowed_users` | 允许使用 bot 的用户 ID 列表（逗号分隔） |
| `ONEBOT11_ALLOW_ALL_USERS` | `allow_all_users` | 是否允许所有用户（true/false） |
| `ONEBOT11_HOME_CHANNEL` | - | Cron 任务投递的默认频道 ID |
| `ONEBOT11_GROUP_ALLOWED_CHATS` | `group_allowed_chats` | 允许进入 gateway 的群号列表（逗号分隔 / YAML 列表） |
| `ONEBOT11_AT_MENTION_ONLY` | `at_mention_only` | 群内是否仅处理 @ 机器人的消息 |
| `ONEBOT11_SILENT_UNAUTHORIZED_DM` | `silent_unauthorized_dm` | 是否静默忽略未授权用户的私聊（不触发配对流程） |
| `ONEBOT11_CONNECT_NOTIFY` | `connect_notify` | 连接成功后通知的 chat_id 列表（逗号分隔 / YAML 列表），每个 ID 收到一条私聊消息 |
| - | `voice_media_max_bytes` | 单条 `record` 语音写入 Gateway 媒体缓存的最大字节数，默认 20 MiB |
| - | `record_path_map` | 将 OneBot 容器内 `get_record` 返回的绝对路径映射到 Gateway 主机路径；值为 `容器路径=主机路径` 的字符串或列表 |

### 语音转写

OneBot adapter 不执行 STT，也不配置或调用本地转写命令。收到 OneBot `record` 段后，适配器会取得语音媒体（优先使用段中的 `url`；只有 `file` 时调用 OneBot `get_record` 请求 WAV），并将它缓存到 `MessageEvent` 的 `VOICE`、`media_urls` 和 `media_types` 字段。Gateway 随后依据全局 `stt.provider` 选择并调用统一的 STT provider。

当 NapCat 在 Docker 容器中运行，`get_record` 可能返回容器内的文件路径，Gateway 主机无法直接读取。请将 NapCat 的语音目录作为卷挂载到 Gateway 主机，并通过 `record_path_map` 将两端的绝对路径对应起来。例如 NapCat 容器中的 `/app/.config/QQ` 挂载到主机的 `/srv/napcat-data` 时：

```yaml
platforms:
  onebot11:
    extra:
      record_path_map:
        - "/app/.config/QQ=/srv/napcat-data"
```

可配置多个映射；路径必须都是绝对路径。映射重叠时，适配器优先使用容器路径前缀最长的一项。

例如，当前使用 FunASR command provider 时，在 Gateway 的 `~/.hermes/config.yaml` 中配置：

```yaml
stt:
  enabled: true
  provider: funasr
  providers:
    funasr:
      type: command
      command: "/path/to/funasr-wrapper --input {input_path} --output {output_path} --language {language}"
      format: txt
      language: zh
      timeout: 300
```

确保 FunASR 所需的模型目录已经存在于 Gateway 主机上；凭据应通过受控的环境变量或凭据管理方式提供，不要写入 `config.yaml`。保存配置后重启 Gateway，并发送一条真实的 OneBot `record` 语音消息验证完整转写链路：

```bash
hermes gateway restart
```

### 群消息过滤顺序

1. 先检查 `group_allowed_chats`
   - 未配置时，默认拒绝所有群消息
   - 只有白名单群会继续进入 gateway
2. 如果开启 `at_mention_only: true`，再检查是否 @ 机器人
3. 最后由 gateway 按 `allowed_users` / `allow_all_users` 检查发送者是否授权

这意味着：

- 白名单外的群消息不会进入 gateway，也不会进入 session
- 白名单群里未 @ 机器人的消息也不会进入 gateway
- 发送功能不受上述过滤影响，仍可正常向群里回消息

### 连接成功通知

配置 `connect_notify` 后，OneBot v11 通道连接成功（收到 `lifecycle/connect` 事件）时，会自动向列表中的每个 chat_id 发送一条私聊通知，内容格式为 `OneBot v11 通道已连接 (bot_id=xxx)`。

```yaml
platforms:
  onebot11:
    extra:
      connect_notify:
        - "402156474"
        - "123456789"
```

环境变量方式（逗号分隔）：

```env
ONEBOT11_CONNECT_NOTIFY=402156474,123456789
```

通知在后台异步发送，有 1 秒延迟确保连接完全就绪，不阻塞其他平台连接。未配置或列表为空时不发送任何通知。

## OneBot 服务器配置

如果你使用 NapCat，需要在 OneBot 配置文件中添加 WebSocket 服务器：

```json
{
  "network": {
    "websocketServers": [
      {
        "enable": true,
        "name": "hermes",
        "host": "0.0.0.0",
        "port": 6097,
        "reportSelfMessage": true,
        "enableForcePushEvent": true,
        "messagePostFormat": "array",
        "token": "your-token",
        "debug": false,
        "heartInterval": 30000
      }
    ]
  }
}
```

如果你使用 Docker 部署 NapCat，记得在 `docker-compose.yml` 中暴露对应的端口：

```yaml
ports:
  - "6097:6097"
```

## 使用

配置完成后，启动 Hermes Gateway：

```bash
hermes gateway
```

在 QQ 上给 bot 发送消息即可开始对话。

首次使用时，发送 `/sethome` 将当前对话设为 home channel。

## 权限说明

- **白名单模式**：设置 `ONEBOT11_ALLOWED_USERS` 后，只有指定用户 ID 可以与 bot 对话
- **全放行模式**：设置 `ONEBOT11_ALLOW_ALL_USERS=true` 后，所有人都可以与 bot 对话
- **静默未授权私聊**：设置 `silent_unauthorized_dm: true` 后，未授权用户的私聊消息被静默丢弃，不触发配对码提示

## 文件结构

```
~/.hermes/plugins/onebot-hermes-adapter/
├── README.md
├── plugin.yaml      # 插件元数据
├── __init__.py      # 入口文件
└── adapter.py       # OneBot v11 适配器实现
```

## 许可证

MIT License
