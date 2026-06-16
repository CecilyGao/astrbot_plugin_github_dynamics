# 更新日志

## [0.1.0] - 2026-06-16
### 优化
- 修复上一版本中的格式问题，这一版本比较稳健。
- 统一了子命令check、pushnow与自动推送的输出格式。


## [0.0.1] - 2026-06-10

### 新增
- 首次发布，融合原 `astrbot_plugin_private_github` 与 `astrbot_plugin_listen_github` 功能。
- **支持监听类型**：
  - GitHub 用户动态（通过 Events API，包含 Push、Issue、PR、Star、Fork 等事件）
  - GitHub 仓库动态（Issues、Commits、Releases）
  - GitHub 组织项目 V2 卡片动态（通过 GraphQL）
- **订阅管理**：
  - 每个会话独立存储订阅列表（`subscriptions.json`）
  - 使用 KV 存储游标，增量获取新动态，避免重复推送
  - 定时轮询所有会话订阅（默认 1800 秒，可配置）
- **命令**：
  - `gh subscribe` – 订阅用户/仓库/项目
  - `gh unsubscribe` – 取消订阅（通过序号）
  - `gh list` – 查看当前会话订阅列表
  - `gh pushnow` – 立即推送当前会话所有订阅的新动态
  - `gh check` – 临时查询目标动态（不订阅）
- **权限控制**：
  - 支持管理员免白名单
  - 支持配置 `whitelist` 限制普通用户使用指令
- **@ 提醒功能**：
  - 可配置 `at_enable` 开启
  - 通过 `username_qq` 映射 GitHub 用户名与 QQ 号
- **时区转换**：支持自定义 `timezone`，将 GitHub 时间转换为本地时间
- **私有仓库支持**：使用 GitHub Personal Access Token（需 `repo`、`read:org`、`user` 权限）
- **配置项**：参考 `_conf_schema.json` 完整定义

### 优化
- 统一命令风格，去除下划线，使用自然空格分隔
- 重构数据持久化，订阅与游标分离存储
- 完善错误处理与日志记录