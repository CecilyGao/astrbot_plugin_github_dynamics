# AstrBot GitHub Dynamics 插件

通过 GitHub API 监听用户动态、仓库动态（Issues/Commits/Releases）及组织项目动态，支持personal access token查询私有仓库，多会话独立订阅与定时推送。

---

## ⚒ 功能特性

- 👤 **用户动态**：监听指定 GitHub 用户的公开活动（Push、Issue、PR、Star、Fork 等）
- 📦 **仓库动态**：监听仓库的 Issues、Commits、Releases
- 🏢 **组织项目**：监听 GitHub Project V2 的卡片更新（新建、内容变更等）
- 🔒 **私有仓库支持**：使用 GitHub Personal Access Token 访问私有数据
- 📢 **多会话独立订阅**：每个群聊/私聊可独立管理自己的订阅列表
- ⏱️ **增量推送**：基于游标记录，只推送新动态，避免重复
- 🔔 **@ 提醒**：可配置将 GitHub 用户名映射到 QQ 号，推送时 @ 相关人员
- ⚙️ **灵活配置**：轮询间隔、最大推送条目、时区、白名单等均可自定义

---

## ⌨️ 安装与配置

### 1. 安装插件

将插件文件夹 `astrbot_plugin_github_dynamics` 放入 AstrBot 的 `addons` 目录，然后重启 AstrBot 或通过 WebUI 重载插件。

### 2. 配置文件

在 AstrBot WebUI 中配置以下参数（或手动编辑 `data/star/astrbot_plugin_github_dynamics/_conf_schema.json` 对应项）：

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `github_token` | string | (空) | **必填**。GitHub Personal Access Token，需具有 `repo`、`read:org`、`user` 权限。 |
| `poll_interval` | int | 1800 | 轮询间隔（秒），建议不低于 600。 |
| `max_entries` | int | 5 | 每次推送每个订阅项最多显示的新动态条数，0 为不限制。 |
| `timezone` | string | `Asia/Shanghai` | 显示动态时间所用的时区。 |
| `at_enable` | bool | `false` | 是否在推送消息中 @ 对应的 QQ 号。 |
| `username_qq` | list | `[]` | GitHub 用户名与 QQ 号的映射列表，格式见下方示例。 |
| `whitelist` | list | `[]` | 允许使用订阅命令的用户 ID 列表，为空则所有用户可用。 |

#### `username_qq` 示例

```json
[
  { "username": "octocat", "qq": "123456789" },
  { "username": "torvalds", "qq": "987654321" }
]
```

## 📋 命令说明
所有命令均以 gh 开头，后跟空格分隔的子命令。可在群聊或私聊中使用。

| 命令 | 用法 | 说明 |
|------|------|------|
| `gh subscribe` | `gh subscribe 用户名`<br>`gh subscribe 仓库名:事件类型`<br>`gh subscribe 组织名/项目编号` | 在当前会话添加一个监听目标。<br>仓库事件类型可选：issues、commits、releases，默认为 commits。<br>组织项目编号可在项目 URL 中看到（如 org/5）。 |
| `gh unsubscribe` | `gh unsubscribe 序号` | 取消当前会话中的某个订阅。序号通过 `gh list` 查看。 |
| `gh list` | `gh list` | 列出当前会话的所有订阅及其序号。 |
| `gh pushnow` | `gh pushnow` | 立即检查当前会话所有订阅的新动态并推送（不等待轮询）。 |
| `gh check` | `gh check 目标` | 临时查询指定目标的最新动态（不添加订阅），目标语法同 `gh subscribe`。 |

提示：命令中的 <目标> 可以是用户登录名、仓库名（owner/repo，可加 :issues,commits,releases等）、组织项目（org/number），例如：

用户登录名：CecilyGao

仓库名：CecilyGao/astrbot_plugin_github_dynamics      事件类型：issues,commits

组织项目：AstrBotDevs/1

```
示例：
gh check CecilyGao
gh subscribe CecilyGao/astrbot_plugin_github_dynamics:issues,commits
gh subscribe facebook/react:releases
gh subscribe AstrBotDevs/1
```

## ⌛ 工作流程

- 用户在某个会话中通过 gh subscribe 添加监听目标，插件会立即初始化游标，记录该目标的“最新一条动态”。

- 后台定时轮询（默认 1800 秒）所有会话的所有订阅，通过 GitHub API 获取新动态。

- 发现新动态后，按订阅格式组装消息，推送到对应会话。用户也可手动使用 gh pushnow 强制立即推送当前会话。

- 使用 gh unsubscribe 可删除不再需要的订阅。

## ‼️ 注意事项

- Token 权限：监听私有仓库需 token 包含 repo 范围；监听组织项目需 read:org；监听用户公开动态仅需 user（但一般 token 默认有基础权限）。

- API 速率限制：使用 Token 时每小时有 5000 次请求额度（普通用户为 60 次）。插件每次轮询会为每个订阅发起若干请求，请合理设置 poll_interval 避免超限。

- 组织项目支持：仅支持 Project V2（即新版项目面板），不支持旧的 Projects (classic)。项目编号可在项目 URL 中看到（/org/projects/数字）。

- 用户动态：只获取用户公开的事件。私有仓库事件需要 token 有相应权限且用户有访问权。

- @ 提醒：需要在配置中开启 at_enable 并提供 username_qq 映射，否则推送消息中不会出现 @。

## ⁉️ 常见问题

- 为什么我订阅了用户，但收不到他的任何动态？

    请确认该用户最近有公开活动。如果是私有仓库的活动，需要 token 有 repo 权限且你的账户能访问该仓库。可以尝试下面的curl检测你的token是否有权访问
```bash
curl -H "Authorization: token ghp_1145141919810" "https://api.github.com/repos/组织名/仓库名/事件（issues, commits, releases）"
```


- 订阅仓库后，推送的内容包含大量旧数据？

    首次订阅时会自动初始化游标，仅获取“最新一条动态”作为基准，后续只会推送新动态。如果仍出现问题，可删除订阅重新添加。


- 如何获取组织项目的编号？

    打开项目板，浏览器地址栏类似 https://github.com/orgs/xxx/projects/5，最后的数字 5 就是项目编号。


- 轮询间隔太短会不会触发 GitHub API 限流？

    每个订阅每次轮询会请求 1 次（或少量分页）。建议至少 600 秒（10 分钟），默认 1800 秒（30 分钟）是安全的。


- 我可以在多个群聊中订阅同一个目标吗？

    可以。每个会话的订阅是独立的，互不影响。

## 📁 开源协议
MIT License © 2026 CecilyGao

## 🙏 致谢

- 本插件融合了astrbot_plugin_private_github与astrbot_plugin_listen_github的设计，感谢原插件作者[aliveriver](https://github.com/aliveriver)与协作者[Kingcq](https://github.com/kingcxp)的贡献！

- 感谢[reminder插件](https://github.com/Foolllll-J/astrbot_plugin_reminder) 的设计思路与架构启发，为本插件的多会话订阅管理提供了重要参考。

- [Astrbot Team](https://github.com/AstrBotDevs/AstrBot)