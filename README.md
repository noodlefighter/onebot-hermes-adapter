# OneBot v11 Adapter for Hermes Agent

一个 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 的 OneBot v11 平台适配器插件，让 Hermes 能够接入任何兼容 OneBot v11 协议的聊天服务器（如 NapCat、go-cqhttp、Lagrange 等）。

## 重要声明

**本仓库 100% 由 AI (Hermes Agent / 小叽) 维护，零人工手写代码。**

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
- 支持图片发送
- 用户白名单 / 全放行权限控制
- Cron 定时任务投递支持

## 安装

### 前置要求

- Hermes Agent (v0.14.0+)
- Python 3.11+
- `websockets` 库

### 安装依赖

```bash
pip install websockets
```

### 安装插件

将以下文件复制到 Hermes Agent 的插件目录：

```bash
cp adapter.py __init__.py plugin.yaml ~/.hermes/hermes-agent/plugins/platforms/onebot11/
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
      allow_all_users: false
```

### 配置说明

| 环境变量 | config.yaml | 说明 |
|---------|-------------|------|
| `ONEBOT11_WS_URL` | `ws_url` | WebSocket 服务器地址 |
| `ONEBOT11_ACCESS_TOKEN` | `access_token` | 访问令牌（可选） |
| `ONEBOT11_ALLOWED_USERS` | `allowed_users` | 允许使用 bot 的用户 ID 列表（逗号分隔） |
| `ONEBOT11_ALLOW_ALL_USERS` | `allow_all_users` | 是否允许所有用户（true/false） |
| `ONEBOT11_HOME_CHANNEL` | - | Cron 任务投递的默认频道 ID |

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

## 文件结构

```
onebot-hermes-adapter/
├── README.md
├── plugin.yaml      # 插件元数据
├── __init__.py      # 入口文件
└── adapter.py       # OneBot v11 适配器实现
```

## 许可证

MIT License
