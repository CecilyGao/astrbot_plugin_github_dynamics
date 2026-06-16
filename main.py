import asyncio
import re
import json
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Dict, Any

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import aiohttp
from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, register, StarTools
from astrbot.api import logger, AstrBotConfig

# ------------------------------- 常量 ---------------------------------
REST_API_BASE = "https://api.github.com"
GRAPHQL_API = "https://api.github.com/graphql"

KV_LAST_CURSOR_PREFIX = "ghd_cursor_"

MAX_SCAN_ENTRIES = 50

RE_REPO = re.compile(r"^[a-zA-Z0-9._-]+/[a-zA-Z0-9._-]+$")
RE_PROJECT_ITEM = re.compile(r"^([a-zA-Z0-9._-]+)/(\d+)$")
RE_USER = re.compile(r"^[a-zA-Z0-9](?:[a-zA-Z0-9]|-(?=[a-zA-Z0-9])){0,38}$")

# ------------------------------- 权限辅助 ------------------------------
def is_user_allowed(plugin, event: AstrMessageEvent) -> bool:
    try:
        if event.is_admin():
            return True
        whitelist = getattr(plugin, "whitelist", None)
        if whitelist is None:
            whitelist = plugin.config.get("whitelist", [])
        if not whitelist:
            return True
        return event.get_sender_id() in whitelist
    except Exception:
        return True

def get_session_id(event: AstrMessageEvent) -> str:
    return event.unified_msg_origin

def get_group_id_from_session(session: str) -> str:
    parts = session.split(":", 2)
    if len(parts) >= 3 and parts[1] == "GroupMessage":
        return parts[2]
    return ""

# ------------------------------- 主插件类 ------------------------------
@register(
    "astrbot_plugin_github_dynamics",
    "CecilyGao",
    "通过 GitHub API 监听用户动态、仓库(Issues/Commits/Releases)及组织项目动态，支持私有仓库，多会话独立订阅与定时推送",
    "0.1.0",
    "https://github.com/CecilyGao/astrbot_plugin_github_dynamics",
)
class GitHubDynamicsPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.github_token: str = config.get("github_token", "")
        if not self.github_token:
            logger.warning("[GitHubDynamics] 未配置 github_token，插件将无法访问私有数据")

        self.poll_interval: int = max(config.get("poll_interval", 1800), 60)
        self.max_entries: int = max(config.get("max_entries", 5), 0)
        self.cfg_timezone: str = config.get("timezone", "Asia/Shanghai")
        self.at_enable: bool = config.get("at_enable", False)
        self.whitelist: List[str] = config.get("whitelist", [])

        self.username_qq_map: Dict[str, str] = self._load_username_qq_map()
        self._user_name_cache: Dict[str, str] = {}  # login -> name
        self.subscriptions: Dict[str, List[Dict]] = {}

        self._poll_task: Optional[asyncio.Task] = None
        self._http_session: Optional[aiohttp.ClientSession] = None

        self._load_subscriptions_from_config()

    # ========================= 用户名-QQ 映射 =========================
    def _load_username_qq_map(self) -> Dict[str, str]:
        raw = self.config.get("username_qq", [])
        if not isinstance(raw, list):
            return {}
        mapping = {}
        for item in raw:
            if not isinstance(item, str):
                continue
            parts = item.split(":", 1)
            if len(parts) != 2:
                continue
            username = parts[0].strip()
            qq = parts[1].strip()
            if username and qq:
                mapping[username.lower()] = qq
        return mapping

    def _resolve_qq_for_username(self, username: str) -> Optional[str]:
        if not username:
            return None
        clean = username.split('@')[0].strip().lower()
        return self.username_qq_map.get(clean)

    # ------------------------- 用户格式化辅助（纯文本，无表情） -------------------------
    @staticmethod
    def _extract_login_name_from_user(user_obj: Dict[str, Any]) -> Tuple[str, Optional[str]]:
        if not user_obj:
            return "", None
        login = user_obj.get("login", "")
        name = user_obj.get("name")
        if name and isinstance(name, str) and name.strip() and name.strip().lower() != "none":
            return login, name.strip()
        return login, None

    def _format_user_display(self, user_obj: Dict[str, Any]) -> str:
        """返回格式：name（login） 或 login，纯文本，不包含任何表情"""
        login, name = self._extract_login_name_from_user(user_obj)
        if not login:
            return ""
        if name:
            return f"{name}（{login}）"
        return login

    def _format_user_display_by_login(self, login: str, name: Optional[str] = None) -> str:
        if not login:
            return ""
        if name:
            return f"{name}（{login}）"
        return login

    async def _fetch_user_name(self, login: str) -> Optional[str]:
        if login in self._user_name_cache:
            return self._user_name_cache[login]
        url = f"{REST_API_BASE}/users/{login}"
        data = await self._rest_api_get(url)
        if data and isinstance(data, list) and len(data) > 0:
            data = data[0]
        if data and isinstance(data, dict):
            name = data.get("name")
            if name and isinstance(name, str) and name.strip() and name.strip().lower() != "none":
                self._user_name_cache[login] = name.strip()
                return name.strip()
        self._user_name_cache[login] = None
        return None

    def _extract_assignee_display_list(self, assignees_field: Any) -> List[str]:
        nodes = []
        if not assignees_field:
            return []
        if isinstance(assignees_field, dict):
            nodes = assignees_field.get("nodes", [])
        elif isinstance(assignees_field, list):
            nodes = assignees_field
        else:
            return []
        result = []
        for u in nodes:
            if not isinstance(u, dict):
                continue
            display = self._format_user_display(u)
            if display:
                result.append(display)
        return result

    # ========================= 数据持久化 =========================
    def _load_subscriptions_from_config(self):
        raw_list = self.config.get("subscriptions", [])
        if not isinstance(raw_list, list):
            raw_list = []
        new_subs = {}
        for item in raw_list:
            template_key = item.get("__template_key", "")
            group_id = str(item.get("group_id", "")).strip()
            if group_id:
                session = f"default:GroupMessage:{group_id}"
            else:
                logger.warning("[GitHubDynamics] 私聊订阅历史数据无法恢复，请重新订阅。")
                continue
            sub = self._config_item_to_sub(item, template_key)
            if sub:
                new_subs.setdefault(session, []).append(sub)
        self.subscriptions = new_subs
        logger.info(f"[GitHubDynamics] 从配置文件加载了 {len(self.subscriptions)} 个会话的订阅")

    def _config_item_to_sub(self, item: dict, template_key: str = "") -> Optional[dict]:
        if template_key == "user_subscription" or (not template_key and item.get("user_id")):
            user_id = item.get("user_id")
            if not user_id:
                return None
            return {"type": "user", "username": user_id}
        elif template_key == "repo_subscription" or (not template_key and item.get("repo_id")):
            repo_id = item.get("repo_id")
            if not repo_id:
                return None
            events = []
            if item.get("issues_enabled", False):
                events.append("issues")
            if item.get("commits_enabled", True):
                events.append("commits")
            if item.get("releases_enabled", False):
                events.append("releases")
            if not events:
                events = ["commits"]
            return {"type": "repo", "repo": repo_id, "events": events}
        elif template_key == "project_subscription" or (not template_key and item.get("organization_id") and item.get("project_id")):
            org = item.get("organization_id")
            proj = item.get("project_id")
            if not org or not proj:
                return None
            return {"type": "project", "org": org, "number": int(proj)}
        else:
            return None

    def _sub_to_config_item(self, sub: dict, session: str) -> dict:
        group_id = get_group_id_from_session(session)
        if sub["type"] == "user":
            return {"__template_key": "user_subscription", "group_id": group_id, "user_id": sub["username"]}
        elif sub["type"] == "repo":
            return {
                "__template_key": "repo_subscription",
                "group_id": group_id,
                "repo_id": sub["repo"],
                "issues_enabled": "issues" in sub["events"],
                "commits_enabled": "commits" in sub["events"],
                "releases_enabled": "releases" in sub["events"],
            }
        elif sub["type"] == "project":
            return {
                "__template_key": "project_subscription",
                "group_id": group_id,
                "organization_id": sub["org"],
                "project_id": str(sub["number"]),
            }
        return {}

    def _sync_subscriptions_to_config(self):
        config_list = []
        for session, subs in self.subscriptions.items():
            for sub in subs:
                config_list.append(self._sub_to_config_item(sub, session))
        self.config["subscriptions"] = config_list
        self.config.save_config()

    # ========================= 游标管理 =========================
    def _get_cursor_key(self, session: str, sub: Dict, event_type: str = None) -> str:
        safe_session = session.replace(":", "_").replace(" ", "")
        if sub["type"] == "user":
            return f"{KV_LAST_CURSOR_PREFIX}{safe_session}_user_{sub['username']}"
        elif sub["type"] == "repo":
            if event_type:
                return f"{KV_LAST_CURSOR_PREFIX}{safe_session}_{sub['repo']}_{event_type}"
            return f"{KV_LAST_CURSOR_PREFIX}{safe_session}_{sub['repo']}"
        else:
            return f"{KV_LAST_CURSOR_PREFIX}{safe_session}_project_{sub['org']}_{sub['number']}"

    async def _get_cursor(self, session: str, sub: Dict, event_type: str = None) -> str:
        key = self._get_cursor_key(session, sub, event_type)
        return await self.get_kv_data(key, "")

    async def _set_cursor(self, session: str, sub: Dict, cursor: str, event_type: str = None):
        key = self._get_cursor_key(session, sub, event_type)
        await self.put_kv_data(key, cursor)

    async def _init_subscription_cursor(self, session: str, sub: Dict):
        try:
            if sub["type"] == "user":
                await self._fetch_user_name(sub["username"])
                latest = await self._fetch_latest_user_event(sub["username"])
                cursor = str(latest.get("id", "")) if latest else "__EMPTY__"
                await self._set_cursor(session, sub, cursor)
            elif sub["type"] == "repo":
                for ev_type in sub["events"]:
                    latest = await self._fetch_latest_repo_entry(sub["repo"], ev_type)
                    cursor = self._extract_cursor_from_entry(latest, ev_type) if latest else "__EMPTY__"
                    await self._set_cursor(session, sub, cursor, ev_type)
            else:
                latest = await self._fetch_latest_project_item(sub["org"], sub["number"])
                cursor = latest.get("raw_updated_at", "2000-01-01T00:00:00Z") if latest else "__EMPTY__"
                await self._set_cursor(session, sub, cursor)
            logger.info(f"[GitHubDynamics] 初始化游标成功: {self._get_cursor_key(session, sub)}")
        except Exception as e:
            logger.error(f"[GitHubDynamics] 初始化游标失败: {e}")

    # ========================= 生命周期 =========================
    async def initialize(self):
        if not self.github_token:
            logger.error("[GitHubDynamics] github_token 未配置，插件将无法正常工作")
        logger.info(f"[GitHubDynamics] 初始化完成，轮询间隔: {self.poll_interval} 秒，已加载 {len(self.subscriptions)} 个会话订阅")
        self._http_session = aiohttp.ClientSession(
            headers={"Authorization": f"token {self.github_token}", "Accept": "application/vnd.github.v3+json"},
            timeout=aiohttp.ClientTimeout(total=30),
        )
        await self._ensure_all_cursors()
        self._poll_task = asyncio.create_task(self._poll_loop())

    async def _ensure_all_cursors(self):
        for session, items in self.subscriptions.items():
            for sub in items:
                if sub["type"] == "repo":
                    for ev_type in sub["events"]:
                        cursor = await self._get_cursor(session, sub, ev_type)
                        if not cursor:
                            await self._init_subscription_cursor(session, sub)
                            break
                else:
                    cursor = await self._get_cursor(session, sub)
                    if not cursor:
                        await self._init_subscription_cursor(session, sub)

    async def terminate(self):
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
        logger.info("[GitHubDynamics] 插件已卸载")

    # ========================= 辅助函数 =========================
    @staticmethod
    def _extract_cursor_from_entry(entry: Dict[str, Any], sub_type: str) -> str:
        if sub_type in ("user", "issues", "releases"):
            return str(entry.get("id", ""))
        elif sub_type in ("commit", "commits"):
            return entry.get("sha", "")
        elif sub_type == "project":
            return entry.get("raw_updated_at", "2000-01-01T00:00:00Z")
        return ""

    def _convert_time(self, time_str: str) -> str:
        if not time_str:
            return ""
        try:
            dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(ZoneInfo(self.cfg_timezone)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return time_str

    # ========================= GitHub API 请求 =========================
    async def _rest_api_get(self, url: str) -> Optional[List[Dict]]:
        if not self._http_session or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                headers={"Authorization": f"token {self.github_token}"},
                timeout=aiohttp.ClientTimeout(total=30),
            )
        try:
            async with self._http_session.get(url) as resp:
                if resp.status == 404:
                    logger.warning(f"[GitHubDynamics] API 404: {url}")
                    return None
                if resp.status != 200:
                    logger.warning(f"[GitHubDynamics] API 请求失败: {url} -> HTTP {resp.status}")
                    return None
                data = await resp.json()
                return data if isinstance(data, list) else [data]
        except Exception as e:
            logger.error(f"[GitHubDynamics] API 请求异常: {url} -> {e}")
            return None

    async def _graphql_request(self, query: str, variables: Dict = None) -> Optional[Dict]:
        if not self._http_session or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                headers={"Authorization": f"token {self.github_token}"},
                timeout=aiohttp.ClientTimeout(total=30),
            )
        try:
            payload = {"query": query}
            if variables:
                payload["variables"] = variables
            async with self._http_session.post(GRAPHQL_API, json=payload) as resp:
                if resp.status != 200:
                    logger.warning(f"[GitHubDynamics] GraphQL 请求失败: HTTP {resp.status}")
                    return None
                data = await resp.json()
                if "errors" in data:
                    logger.warning(f"[GitHubDynamics] GraphQL 错误: {data['errors']}")
                    return None
                return data.get("data")
        except Exception as e:
            logger.error(f"[GitHubDynamics] GraphQL 请求异常: {e}")
            return None

    # ========================= 用户动态 =========================
    async def _fetch_user_events(self, username: str, per_page: int = 30) -> Optional[List[Dict]]:
        url = f"{REST_API_BASE}/users/{username}/events?per_page={per_page}"
        return await self._rest_api_get(url)

    async def _fetch_latest_user_event(self, username: str) -> Optional[Dict]:
        events = await self._fetch_user_events(username, per_page=1)
        return events[0] if events else None

    async def _fetch_new_user_entries(self, username: str, last_cursor: str) -> Tuple[List[Dict], str]:
        events = await self._fetch_user_events(username, per_page=MAX_SCAN_ENTRIES)
        if not events:
            return [], last_cursor

        new_entries = []
        for event in events:
            event_id = str(event.get("id", ""))
            if event_id == last_cursor:
                break
            entry = await self._build_user_entry_dict(event, username)
            if entry:
                new_entries.append(entry)

        if self.max_entries > 0 and len(new_entries) > self.max_entries:
            new_entries = new_entries[: self.max_entries]

        if new_entries and events:
            latest_cursor = str(events[0].get("id", ""))
            return new_entries, latest_cursor
        return new_entries, last_cursor

    async def _build_user_entry_dict(self, event: Dict, username: str) -> Dict:
        event_type = event.get("type", "Unknown")
        repo = event.get("repo", {}).get("name", "unknown/repo")
        created_at = event.get("created_at", "")
        payload = event.get("payload", {})

        title = f"[{event_type}] {repo}"
        link = f"https://github.com/{repo}"
        content = ""
        updates = []

        if event_type == "PushEvent":
            commits = payload.get("commits", [])
            cnt = len(commits)
            content = f"推送了 {cnt} 个提交"
            for commit in commits[:3]:
                sha = commit.get("sha", "")[:7]
                msg = commit.get("message", "").splitlines()[0][:60]
                updates.append(f"📌 {sha}: {msg}")
            if cnt > 3:
                updates.append(f"... 还有 {cnt-3} 个提交")
        elif event_type == "IssuesEvent":
            action = payload.get("action", "")
            issue = payload.get("issue", {})
            number = issue.get("number", "")
            title_issue = issue.get("title", "")[:80]
            content = f"{action} issue #{number}: {title_issue}"
            link = issue.get("html_url", link)
            updates.append(f"🔖 {action} 操作")
        elif event_type == "PullRequestEvent":
            action = payload.get("action", "")
            pr = payload.get("pull_request", {})
            number = pr.get("number", "")
            title_pr = pr.get("title", "")[:80]
            content = f"{action} PR #{number}: {title_pr}"
            link = pr.get("html_url", link)
            updates.append(f"🔀 {action} 操作")
        elif event_type == "CreateEvent":
            ref_type = payload.get("ref_type", "")
            ref = payload.get("ref", "")
            content = f"创建了 {ref_type} {ref}".strip()
            updates.append(f"✨ 创建了 {ref_type}")
        elif event_type == "WatchEvent":
            content = "Star 了仓库"
            updates.append("⭐ Star 了仓库")
        elif event_type == "ForkEvent":
            forkee = payload.get("forkee", {})
            content = f"Fork 了仓库到 {forkee.get('full_name', '')}"
            link = forkee.get("html_url", link)
            updates.append("🍴 Fork 了仓库")
        else:
            content = f"触发了 {event_type} 事件"
            updates.append(f"⚡ 触发 {event_type}")

        user_name = await self._fetch_user_name(username)
        author_display = self._format_user_display_by_login(username, user_name)

        return {
            "title": title,
            "link": link,
            "published": self._convert_time(created_at),
            "content": content,
            "updates": updates,
            "id": str(event.get("id", "")),
            "type": "user_event",
            "author_display": author_display,
            "author_login": username,
            "assignees_display": [],
        }

    # ========================= 仓库动态 =========================
    async def _fetch_latest_repo_entry(self, repo: str, event_type: str) -> Optional[Dict]:
        url = self._build_repo_api_url(repo, event_type, per_page=1)
        data = await self._rest_api_get(url)
        if data:
            return await self._build_repo_entry_dict(data[0], event_type)
        return None

    async def _fetch_new_repo_entries(self, repo: str, event_type: str, last_cursor: str) -> Tuple[List[Dict], str]:
        url = self._build_repo_api_url(repo, event_type, per_page=MAX_SCAN_ENTRIES)
        items = await self._rest_api_get(url)
        if not items:
            return [], last_cursor

        new_entries = []
        for item in items:
            cursor = self._extract_cursor_from_entry(item, event_type)
            if cursor == last_cursor:
                break
            entry = await self._build_repo_entry_dict(item, event_type)
            if entry:
                new_entries.append(entry)

        if self.max_entries > 0 and len(new_entries) > self.max_entries:
            new_entries = new_entries[: self.max_entries]

        if new_entries and items:
            latest_cursor = self._extract_cursor_from_entry(items[0], event_type)
            return new_entries, latest_cursor
        return new_entries, last_cursor

    def _build_repo_api_url(self, repo: str, event_type: str, per_page: int = 30) -> str:
        base = f"{REST_API_BASE}/repos/{repo}"
        if event_type == "issues":
            return f"{base}/issues?state=all&sort=created&direction=desc&per_page={per_page}"
        elif event_type == "commits":
            return f"{base}/commits?per_page={per_page}"
        elif event_type == "releases":
            return f"{base}/releases?per_page={per_page}"
        raise ValueError(f"Unknown event_type: {event_type}")

    async def _build_repo_entry_dict(self, raw: Dict, event_type: str) -> Dict:
        if event_type == "issues":
            user_obj = raw.get("user") or {}
            login, _ = self._extract_login_name_from_user(user_obj)
            if login:
                name = await self._fetch_user_name(login)
                author_display = self._format_user_display_by_login(login, name)
            else:
                author_display = ""
        # 处理指派人
            assignees_raw = raw.get("assignees") or []
            assignees_display = []
            for u in assignees_raw:
                ulogin, _ = self._extract_login_name_from_user(u)
                if ulogin:
                    uname = await self._fetch_user_name(ulogin)
                    assignees_display.append(self._format_user_display_by_login(ulogin, uname))
            updates = [f"📝 创建于 {self._convert_time(raw['created_at'])}"]
            if raw.get("state") == "closed":
                updates.append("🔒 状态：已关闭")
            else:
                updates.append("🟢 状态：开放中")
            return {
                "title": f"[Issue] #{raw['number']}: {raw['title']}",
                "link": raw["html_url"],
                "published": self._convert_time(raw["created_at"]),
                "content": (raw.get("body") or "")[:200],
                "updates": updates,
                "id": str(raw["id"]),
                "type": "issue",
                "author_display": author_display,
                "author_login": login or "",
                "assignees_display": assignees_display,
            }
        elif event_type == "commits":
            commit = raw.get("commit", {})
            message = commit.get("message", "")
            short_msg = message.splitlines()[0][:100]
            updates = [f"💬 {short_msg}"]
            return {
                "title": f"[Commit] {raw['sha'][:7]}: {short_msg}",
                "link": raw["html_url"],
                "published": self._convert_time(commit.get("committer", {}).get("date", "")),
                "content": message.replace("\n", " ")[:200],
                "updates": updates,
                "id": raw["sha"],
                "type": "commit",
                "author_display": "",
                "author_login": "",
                "assignees_display": [],
            }
        elif event_type == "releases":
            updates = [f"🏷️ 版本 {raw['tag_name']} 发布"]
            return {
                "title": f"[Release] {raw['tag_name']}: {raw.get('name') or raw['tag_name']}",
                "link": raw["html_url"],
                "published": self._convert_time(raw.get("published_at") or raw["created_at"]),
                "content": (raw.get("body") or "")[:200],
                "updates": updates,
                "id": str(raw["id"]),
                "type": "release",
                "author_display": "",
                "author_login": "",
                "assignees_display": [],
            }
        return {}

    # ========================= 组织项目 =========================
    async def _fetch_latest_project_item(self, org: str, number: int) -> Optional[Dict]:
        items = await self._fetch_project_items(org, number, first=1)
        return items[0] if items else None

    async def _fetch_project_items(self, org: str, number: int, first: int = 50) -> List[Dict]:
        query = """
        query($org: String!, $number: Int!, $first: Int!) {
            organization(login: $org) {
                projectV2(number: $number) {
                    id title number
                    items(first: $first) {
                        nodes {
                            id createdAt updatedAt
                            content {
                                __typename
                                ... on Issue {
                                    id number title url bodyText state
                                    createdAt updatedAt
                                    author { login ... on User { name } }
                                    assignees(first: 50) { nodes { login ... on User { name } } }
                                    comments(last: 10) {
                                        nodes { author { login ... on User { name } } bodyText createdAt }
                                    }
                                    timelineItems(last: 15) {
                                        nodes {
                                            __typename
                                            ... on ClosedEvent {
                                                createdAt
                                                actor { login ... on User { name } }
                                            }
                                            ... on ReopenedEvent {
                                                createdAt
                                                actor { login ... on User { name } }
                                            }
                                            ... on AssignedEvent {
                                                createdAt
                                                actor { login ... on User { name } }
                                                assignee { ... on User { login name } }
                                            }
                                        }
                                    }
                                }
                                ... on PullRequest {
                                    id number title url bodyText state merged mergedAt
                                    createdAt updatedAt
                                    author { login ... on User { name } }
                                    assignees(first: 50) { nodes { login ... on User { name } } }
                                    comments(last: 10) {
                                        nodes { author { login ... on User { name } } bodyText createdAt }
                                    }
                                    timelineItems(last: 15) {
                                        nodes {
                                            __typename
                                            ... on ClosedEvent {
                                                createdAt
                                                actor { login ... on User { name } }
                                            }
                                            ... on ReopenedEvent {
                                                createdAt
                                                actor { login ... on User { name } }
                                            }
                                            ... on MergedEvent {
                                                createdAt
                                                actor { login ... on User { name } }
                                            }
                                            ... on AssignedEvent {
                                                createdAt
                                                actor { login ... on User { name } }
                                                assignee { ... on User { login name } }
                                            }
                                        }
                                    }
                                }
                                ... on DraftIssue {
                                    id title bodyText createdAt updatedAt
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        variables = {"org": org, "number": number, "first": first}
        data = await self._graphql_request(query, variables)
        if not data:
            return []
        org_data = data.get("organization")
        if not org_data:
            return []
        project = org_data.get("projectV2")
        if not project:
            return []
        items = project.get("items", {}).get("nodes", [])
        items.sort(key=lambda x: x.get("updatedAt", ""), reverse=True)
        return items

    async def _fetch_new_project_entries(self, org: str, number: int, last_cursor: str) -> Tuple[List[Dict], str]:
        items = await self._fetch_project_items(org, number, first=MAX_SCAN_ENTRIES)
        if not items:
            return [], last_cursor

        new_entries = []
        max_time = last_cursor
        for item in items:
            entry = await self._build_project_entry_dict(item, org, number, last_cursor)
            if not entry:
                continue
            entry_time = entry.get("raw_updated_at", "")
            if entry_time > last_cursor:
                new_entries.append(entry)
                if entry_time > max_time:
                    max_time = entry_time

        if self.max_entries > 0 and len(new_entries) > self.max_entries:
            new_entries = new_entries[-self.max_entries :]

        return new_entries, max_time

    async def _build_project_entry_dict(self, item: Dict, org: str, number: int, last_cursor: str = "") -> Dict:
        content = item.get("content")
        if not content:
            return {}

        typename = content.get("__typename")
        title = content.get("title", "无标题")
        url = content.get("url", "")
        item_updated_at = item.get("updatedAt", "")
        content_updated_at = content.get("updatedAt", "")
        content_created_at = content.get("createdAt", "")
        valid_times = [t for t in [item_updated_at, content_updated_at, content_created_at] if t]

        updates = []
        actors = set()
        author_display = ""
        author_login = ""
        assignees_display = []

        def is_new(ts):
            if not ts:
                return False
            if not last_cursor or last_cursor == "__EMPTY__":
                return True
            return ts > last_cursor

        if typename in ("Issue", "PullRequest"):
            number_str = f"#{content.get('number', '')}"
            item_type = "Issue" if typename == "Issue" else "PR"
            author_obj = content.get("author") or {}
            author_display = self._format_user_display(author_obj)
            author_login, _ = self._extract_login_name_from_user(author_obj)
            assignees_display = self._extract_assignee_display_list(content.get("assignees") or {})
            state = content.get("state", "")
            merged = bool(content.get("merged", False))

            if is_new(content_created_at):
                updates.append(f"🆕 新建卡片：{author_display}")
                if author_login:
                    actors.add(author_login)

            comments = content.get("comments", {}).get("nodes", []) or []
            for c in comments:
                c_time = c.get("createdAt", "")
                if is_new(c_time):
                    c_author_obj = c.get("author") or {}
                    c_author_display = self._format_user_display(c_author_obj)
                    c_author_login, _ = self._extract_login_name_from_user(c_author_obj)
                    snippet = (c.get("bodyText", "") or "").replace("\n", " ")[:200]
                    updates.append(f"💬 评论：{c_author_display}：{snippet}‎")
                    if c_author_login:
                        actors.add(c_author_login)
                    valid_times.append(c_time)

            timeline = content.get("timelineItems", {}).get("nodes", []) or []
            for evt in timeline:
                if not evt:
                    continue
                e_time = evt.get("createdAt", "")
                if is_new(e_time):
                    e_type = evt.get("__typename", "")
                    e_actor_obj = evt.get("actor") or {}
                    e_actor_display = self._format_user_display(e_actor_obj)
                    e_actor_login, _ = self._extract_login_name_from_user(e_actor_obj)
                    if e_type == "ClosedEvent":
                        updates.append(f"🔒 关闭了卡片：{e_actor_display}‎")
                        if e_actor_login:
                            actors.add(e_actor_login)
                    elif e_type == "ReopenedEvent":
                        updates.append(f"🔓 重新打开了卡片：{e_actor_display}‎")
                        if e_actor_login:
                            actors.add(e_actor_login)
                    elif e_type == "MergedEvent":
                        updates.append(f"🎉 合并了 PR：{e_actor_display}‎")
                        if e_actor_login:
                            actors.add(e_actor_login)
                    elif e_type == "AssignedEvent":
                        assignee_obj = evt.get("assignee") or {}
                        assignee_display = self._format_user_display(assignee_obj)
                        updates.append(f"👤 指派给了：{assignee_display}‎")
                    valid_times.append(e_time)

            if not updates and last_cursor and last_cursor != "__EMPTY__":
                updates.append("📌 状态/属性更新")

        elif typename == "DraftIssue":
            item_type = "Draft Issue"
            number_str = ""
            if is_new(content_created_at):
                updates.append(f"🆕 新建草稿卡片：{author_display}‎")
        else:
            item_type = "Card"
            number_str = ""
            if is_new(item_updated_at) and is_new(content_created_at):
                updates.append("🆕 新建卡片")

        last_active_time = max(valid_times) if valid_times else (content_updated_at or item_updated_at or content_created_at)
        primary_actor = list(actors)[0] if actors else author_login

        return {
            "title": f"[{item_type} {number_str}] {title}".replace("[] ", ""),
            "link": url,
            "published": self._convert_time(last_active_time),
            "raw_updated_at": last_active_time,
            "id": item.get("id", ""),
            "type": "project_item",
            "author_display": author_display,
            "author_login": author_login,
            "assignees_display": assignees_display,
            "updates": updates,
            "actor": primary_actor,
        }

    # ========================= 轮询与推送 =========================
    async def _poll_loop(self):
        await asyncio.sleep(10)
        while True:
            try:
                await self._do_poll()
            except asyncio.CancelledError:
                logger.info("[GitHubDynamics] 轮询任务已取消")
                return
            except Exception as e:
                logger.error(f"[GitHubDynamics] 轮询出错: {e}")
            await asyncio.sleep(self.poll_interval)

    async def _do_poll(self):
        if not self.github_token:
            return

        session_messages: Dict[str, List[str]] = {}

        for session, items in list(self.subscriptions.items()):
            for sub in items:
                try:
                    if sub["type"] == "user":
                        last_cursor = await self._get_cursor(session, sub)
                        if not last_cursor:
                            await self._init_subscription_cursor(session, sub)
                            last_cursor = await self._get_cursor(session, sub)
                            if not last_cursor:
                                continue
                        new_entries, new_cursor = await self._fetch_new_user_entries(sub["username"], last_cursor)
                        if new_entries:
                            msg = await self._format_user_entries(sub["username"], new_entries)
                            if msg:
                                session_messages.setdefault(session, []).append(msg)
                        if new_cursor and new_cursor != last_cursor:
                            await self._set_cursor(session, sub, new_cursor)

                    elif sub["type"] == "repo":
                        all_new_entries = []
                        for ev_type in sub["events"]:
                            last_cursor = await self._get_cursor(session, sub, ev_type)
                            if not last_cursor:
                                await self._init_subscription_cursor(session, sub)
                                last_cursor = await self._get_cursor(session, sub, ev_type)
                                if not last_cursor:
                                    continue
                            new_entries, new_cursor = await self._fetch_new_repo_entries(sub["repo"], ev_type, last_cursor)
                            if new_entries:
                                all_new_entries.extend(new_entries)
                            if new_cursor and new_cursor != last_cursor:
                                await self._set_cursor(session, sub, new_cursor, ev_type)
                        if all_new_entries:
                            all_new_entries.sort(key=lambda x: x.get("published_raw", x.get("published", "")), reverse=True)
                            unique = {e["id"]: e for e in all_new_entries}.values()
                            limited = list(unique)[: self.max_entries] if self.max_entries > 0 else list(unique)
                            msg = self._format_repo_entries(sub["repo"], sub["events"], limited)
                            if msg:
                                session_messages.setdefault(session, []).append(msg)

                    else:
                        last_cursor = await self._get_cursor(session, sub)
                        if not last_cursor:
                            await self._init_subscription_cursor(session, sub)
                            last_cursor = await self._get_cursor(session, sub)
                            if not last_cursor:
                                continue
                        new_entries, new_cursor = await self._fetch_new_project_entries(sub["org"], sub["number"], last_cursor)
                        if new_entries:
                            new_entries.sort(key=lambda x: x.get("raw_updated_at", ""), reverse=True)
                            msg = await self._format_project_entries(f"{sub['org']}/{sub['number']}", new_entries)
                            if msg:
                                session_messages.setdefault(session, []).append(msg)
                        if new_cursor and new_cursor != last_cursor:
                            await self._set_cursor(session, sub, new_cursor)

                except Exception as e:
                    logger.error(f"[GitHubDynamics] 处理订阅 {sub} 失败: {e}")

        for session, msg_list in session_messages.items():
            full_msg = "\n\n".join(msg_list)
            full_msg = self._ensure_blank_between_sections(full_msg)
            await self._send_message_with_mentions(session, full_msg)

    # ========================= 消息发送（唯一添加 的地方，并先清除旧表情） =========================
    async def _send_message_with_mentions(self, session: str, full_msg: str):
        chain = MessageChain()
        lines = full_msg.splitlines(keepends=True)

        pattern = re.compile(r'（([a-zA-Z0-9](?:[a-zA-Z0-9]|-(?=[a-zA-Z0-9])){0,38})）')

        for line in lines:
            if not self.at_enable:
                chain.message(line)
                continue

            last_end = 0
            for match in pattern.finditer(line):
                start, end = match.span()
                if start > last_end:
                    chain.message(line[last_end:start])
                login = match.group(1)
                chain.message(f"（{login}）")
                qq = self._resolve_qq_for_username(login)
                if qq:
                    chain.at(login, qq)
                    chain.message(" ‎")
                last_end = end
            if last_end < len(line):
                chain.message(line[last_end:])

        try:
            await self.context.send_message(session, chain)
        except Exception as e:
            logger.error(f"[GitHubDynamics] 推送到 {session} 失败: {e}")

    # ========================= 强制获取最新动态（用于 pushnow） =========================
    async def _fetch_latest_entries_force(self, sub: Dict) -> List[Dict]:
        entries = []
        if sub["type"] == "user":
            events = await self._fetch_user_events(sub["username"], per_page=self.max_entries or 10)
            if events:
                for ev in events[: self.max_entries or 10]:
                    entry = await self._build_user_entry_dict(ev, sub["username"])
                    if entry:
                        entries.append(entry)
        elif sub["type"] == "repo":
            for ev_type in sub["events"]:
                url = self._build_repo_api_url(sub["repo"], ev_type, per_page=self.max_entries or 10)
                items = await self._rest_api_get(url)
                if items:
                    for item in items[: self.max_entries or 10]:
                        entry = await self._build_repo_entry_dict(item, ev_type)
                        if entry:
                            entries.append(entry)
        else:
            items = await self._fetch_project_items(sub["org"], sub["number"], first=self.max_entries or 10)
            if items:
                for item in items[: self.max_entries or 10]:
                    entry = await self._build_project_entry_dict(item, sub["org"], sub["number"])
                    if entry:
                        entries.append(entry)
        unique = {}
        for e in entries:
            unique[e["id"]] = e
        result = list(unique.values())
        if sub["type"] == "project":
            result.sort(key=lambda x: x.get("raw_updated_at", ""), reverse=True)
        else:
            result.sort(key=lambda x: x.get("published_raw", x.get("published", "")), reverse=True)
        return result[: self.max_entries] if self.max_entries > 0 else result

    def _ensure_blank_between_sections(self, text: str) -> str:
        SECTION_EMOJIS = {'👤', '📢', '🐛', '📦', '🖥️'}
        lines = text.splitlines()
        new_lines = []
        first_section = True
        for line in lines:
            stripped = line.strip()
            is_section_start = (
                stripped and 
                stripped[0] in SECTION_EMOJIS and
                not stripped.startswith('➤')
            )
            if is_section_start:
                if first_section:
                    first_section = False
                else:
                    new_lines.append('\n————————————————\n')
            new_lines.append(line)
        return '\n'.join(new_lines)

    # ========================= 消息格式化（纯文本，无表情） =========================
    async def _format_user_entries(self, username: str, entries: List[Dict]) -> str:
        display_name = entries[0].get("author_display", "") if entries else username
        if not display_name:
            name = await self._fetch_user_name(username)
            display_name = self._format_user_display_by_login(username, name)
        lines = [f"👤 用户 {display_name} 的最新动态（{len(entries)} 条）：\n"]
        for i, e in enumerate(entries, 1):
            lines.append(f"  {i}. {e['title']}")
            if e.get("published"):
                lines.append(f"     🕐 时间：{e['published']}")
            if e.get("content"):
                lines.append(f"     📝 {e['content']}")
            for upd in e.get("updates", []):
                lines.append(f"     ➤ {upd}‎")
            author = e.get("author_display", "")
            if author:
                lines.append(f"     🙋 提出者：{author}‎")
            if e.get("link"):
                lines.append(f"     🔗 {e['link']}")
            lines.append("")
        return "\n".join(lines)

    def _format_repo_entries(self, repo: str, event_types: List[str], entries: List[Dict]) -> str:
        icon_map = {"issues": "🐛", "commits": "🖥️", "releases": "📦"}
        icons = " ".join([icon_map.get(et, "🔔") for et in event_types])
        lines = [f"{icons} 仓库 {repo} 的最新动态（{len(entries)} 条）：\n"]
        for i, e in enumerate(entries, 1):
            lines.append(f"  {i}. {e['title']}")
            if e.get("published"):
                lines.append(f"     🕐 时间：{e['published']}")
            if e.get("content"):
                lines.append(f"     📝 详情：{e['content']}")
            for upd in e.get("updates", []):
                lines.append(f"     ➤ {upd}‎")
            author = e.get("author_display", "")
            if author:
                lines.append(f"     🙋 提出者：{author}‎")
            if e.get("assignees_display"):
                ass_str = "，".join(e["assignees_display"])
                lines.append(f"     🔔 指派给：{ass_str}‎")
            if e.get("link"):
                lines.append(f"     🔗 {e['link']}")
            lines.append("")
        return "\n".join(lines)

    async def _format_project_entries(self, project_id: str, entries: List[Dict]) -> str:
        lines = [f"📢 组织项目 {project_id} 的最新动态（{len(entries)} 条）：\n"]
        for i, e in enumerate(entries, 1):
            lines.append(f"  {i}. {e['title']}")
            if e.get("published"):
                lines.append(f"     🕐 时间：{e['published']}")
            for upd in e.get("updates", []):
                lines.append(f"     ➤ {upd}‎")
            author = e.get("author_display", "")
            if author:
                lines.append(f"‎     🙋 提出者：{author}‎")
            if e.get("assignees_display"):
                ass_str = "，".join(e["assignees_display"])
                lines.append(f"     🔔 指派给：{ass_str}‎")
            if e.get("link"):
                lines.append(f"     🔗 {e['link']}")
            lines.append("")
        return "\n".join(lines)

    # ========================= 订阅管理命令 =========================
    async def _cmd_subscribe(self, event: AstrMessageEvent, target: str):
        if not is_user_allowed(self, event):
            yield event.plain_result("❌ 你没有权限使用此指令")
            return
        session = get_session_id(event)

        sub = None
        display = ""

        if RE_PROJECT_ITEM.match(target):
            match = RE_PROJECT_ITEM.match(target)
            org, num = match.group(1), int(match.group(2))
            sub = {"type": "project", "org": org, "number": num}
            display = f"项目 {org}/{num}"
        elif "/" in target:
            repo_part = target
            events_part = "commits"
            if ":" in target:
                repo_part, events_part = target.split(":", 1)
            if not RE_REPO.match(repo_part):
                yield event.plain_result("❌ 仓库格式不正确，应为 owner/repo")
                return
            event_list = [e.strip().lower() for e in events_part.split(",") if e.strip()]
            events = {"issues": False, "commits": False, "releases": False}
            for e in event_list:
                if e in events:
                    events[e] = True
                else:
                    yield event.plain_result(f"❌ 无效的事件类型: {e}，可选: issues, commits, releases")
                    return
            if not any(events.values()):
                events["commits"] = True
            sub = {"type": "repo", "repo": repo_part, "events": [e for e, enabled in events.items() if enabled]}
            display = f"仓库 {repo_part} 的 {', '.join(sub['events'])}"
        else:
            if not RE_USER.match(target):
                yield event.plain_result("❌ 用户名格式不正确")
                return
            sub = {"type": "user", "username": target}
            display = f"用户 {target}"

        for existing in self.subscriptions.get(session, []):
            if existing == sub:
                yield event.plain_result(f"⚠️ 当前会话已订阅 {display}")
                return

        if session not in self.subscriptions:
            self.subscriptions[session] = []
        self.subscriptions[session].append(sub)
        self._sync_subscriptions_to_config()
        await self._init_subscription_cursor(session, sub)
        yield event.plain_result(f"✅ 已成功订阅：{display}")

    async def _cmd_unsubscribe(self, event: AstrMessageEvent, index: Optional[str] = None):
        if not is_user_allowed(self, event):
            yield event.plain_result("❌ 你没有权限使用此指令")
            return
        session = get_session_id(event)
        items = self.subscriptions.get(session, [])
        if not items:
            yield event.plain_result("当前会话没有任何订阅")
            return

        if index is None:
            msg = "📋 当前会话的订阅列表：\n"
            for i, sub in enumerate(items, 1):
                msg += f"{i}. {self._format_sub(sub)}\n"
            msg += "请使用 gh unsubscribe <序号> 取消对应的订阅"
            yield event.plain_result(msg)
            return

        try:
            idx = int(index) - 1
            if idx < 0 or idx >= len(items):
                yield event.plain_result(f"❌ 序号无效，请输入 1-{len(items)} 之间的数字")
                return
            removed = items.pop(idx)
            if not items:
                del self.subscriptions[session]
            self._sync_subscriptions_to_config()
            yield event.plain_result(f"✅ 已取消订阅：{self._format_sub(removed)}")
        except ValueError:
            yield event.plain_result("❌ 序号必须为数字")

    async def _cmd_list(self, event: AstrMessageEvent):
        if not is_user_allowed(self, event):
            yield event.plain_result("❌ 你没有权限使用此指令")
            return
        session = get_session_id(event)
        items = self.subscriptions.get(session, [])
        if not items:
            yield event.plain_result("当前会话没有任何订阅。使用 gh subscribe 添加订阅。")
            return
        msg = "📋 当前会话的订阅列表：\n"
        for i, sub in enumerate(items, 1):
            msg += f"{i}. {self._format_sub(sub)}\n"
        yield event.plain_result(msg)

    async def _cmd_pushnow(self, event: AstrMessageEvent):
        if not is_user_allowed(self, event):
            yield event.plain_result("❌ 你没有权限使用此指令")
            return
        if not self.github_token:
            yield event.plain_result("❌ 未配置 github_token，无法执行推送")
            return
        session = get_session_id(event)
        items = self.subscriptions.get(session, [])
        if not items:
            yield event.plain_result("当前会话没有任何订阅，请先使用 gh subscribe 添加订阅。")
            return
        yield event.plain_result("🔄 正在获取已订阅目标的最新动态...")

        msg_list = []
        for sub in items:
            entries = await self._fetch_latest_entries_force(sub)
            if entries:
                if sub["type"] == "user":
                    msg = await self._format_user_entries(sub["username"], entries)
                elif sub["type"] == "repo":
                    msg = self._format_repo_entries(sub["repo"], sub["events"], entries)
                else:
                    msg = await self._format_project_entries(f"{sub['org']}/{sub['number']}", entries)
                if msg:
                    msg_list.append(msg)

        if msg_list:
            full_msg = "\n\n".join(msg_list)
            full_msg = self._ensure_blank_between_sections(full_msg) 
            await self._send_message_with_mentions(session, full_msg)
        else:
            yield event.plain_result("✅ 所有订阅均无最新动态。")

    async def _cmd_check(self, event: AstrMessageEvent, target: str):
        if not is_user_allowed(self, event):
            yield event.plain_result("❌ 你没有权限使用此指令")
            return

        session = get_session_id(event)
        if RE_PROJECT_ITEM.match(target):
            match = RE_PROJECT_ITEM.match(target)
            org, num = match.group(1), int(match.group(2))
            yield event.plain_result(f"🔄 正在查询项目 {org}/{num} ...")
            items = await self._fetch_project_items(org, num, first=self.max_entries or 10)
            if not items:
                yield event.plain_result("❌ 无法获取项目数据，请检查组织名、项目编号及 token 权限")
                return
            entries = []
            for item in items[: self.max_entries or 10]:
                entry = await self._build_project_entry_dict(item, org, num)
                if entry:
                    entries.append(entry)
            if entries:
                entries.sort(key=lambda x: x.get("raw_updated_at", ""), reverse=True)
                msg = await self._format_project_entries(f"{org}/{num}", entries)
                await self._send_message_with_mentions(session, msg)
            else:
                await self._send_message_with_mentions(session, f"🔍 项目 {org}/{num} 暂无最近动态。")
            return

        if "/" in target:
            repo_part = target
            events_part = "commits"
            if ":" in target:
                repo_part, events_part = target.split(":", 1)
            if not RE_REPO.match(repo_part):
                yield event.plain_result("❌ 仓库格式不正确，应为 owner/repo")
                return
            event_list = [e.strip().lower() for e in events_part.split(",") if e.strip()]
            events = {"issues": False, "commits": False, "releases": False}
            for e in event_list:
                if e in events:
                    events[e] = True
                else:
                    yield event.plain_result(f"❌ 无效的事件类型: {e}，可选: issues, commits, releases")
                    return
            if not any(events.values()):
                events["commits"] = True
            enabled_events = [e for e, en in events.items() if en]

            all_entries = []
            for ev_type in enabled_events:
                yield event.plain_result(f"🔄 正在查询仓库 {repo_part} 的 {ev_type} ...")
                url = self._build_repo_api_url(repo_part, ev_type, per_page=self.max_entries or 10)
                items = await self._rest_api_get(url)
                if items:
                    for item in items[: self.max_entries or 10]:
                        entry = await self._build_repo_entry_dict(item, ev_type)
                        if entry:
                            all_entries.append(entry)
            if all_entries:
                unique = {e["id"]: e for e in all_entries}.values()
                limited = list(unique)[: self.max_entries or 10]
                limited.sort(key=lambda x: x.get("published", ""), reverse=True)
                msg = self._format_repo_entries(repo_part, enabled_events, limited)
                await self._send_message_with_mentions(session, msg)
            else:
                await self._send_message_with_mentions(session, f"🔍 仓库 {repo_part} 暂无最近的动态。")
            return

        if not RE_USER.match(target):
            yield event.plain_result("❌ 用户名格式不正确")
            return
        yield event.plain_result(f"🔄 正在查询用户 {target} 的最新动态...")
        events = await self._fetch_user_events(target, per_page=self.max_entries or 10)
        if not events:
            yield event.plain_result(f"🔍 用户 {target} 暂无最近公开动态。")
            return
        entries = []
        for ev in events[: self.max_entries or 10]:
            entry = await self._build_user_entry_dict(ev, target)
            if entry:
                entries.append(entry)
        if entries:
            entries.sort(key=lambda x: x.get("published", ""), reverse=True)
            msg = await self._format_user_entries(target, entries)
            await self._send_message_with_mentions(session, msg)
        else:
            await self._send_message_with_mentions(session, f"🔍 用户 {target} 暂无最近动态。")

    async def _cmd_help(self, event: AstrMessageEvent):
        help_text = (
            "📦 GitHub Dynamics 插件 v0.1.0\n"
            "命令格式：gh 子命令 [参数]\n\n"
            "可用子命令：\n"
            "👉subscribe 目标    - 订阅目标\n"
            "👉unsubscribe 序号    - 取消订阅\n"
            "👉list    - 列出当前会话的订阅\n"
            "👉pushnow    - 在当前会话立即推送所有订阅的最新动态\n"
            "👉check 目标    - 查询目标的最新动态（不订阅）\n"
            "👉help    - 显示本帮助\n\n"
            "目标格式：\n"
            "- 用户：CecilyGao\n"
            "- 仓库：octocat/Hello-World[:events]   (events可选 issues,commits,releases，默认 commits，多选用逗号分隔)\n"
            "- 项目：octocat/123 (即组织名/项目编号)\n\n"
            "示例：\n"
            "- gh check CecilyGao\n"
            "- gh subscribe CecilyGao/astrbot_plugin_github_dynamics:issues,commits\n"
            "- gh subscribe facebook/react:releases\n"
            "- gh subscribe AstrBotDevs/1\n"
            "数据来源：GitHub API"
        )
        yield event.plain_result(help_text)

    def _format_sub(self, sub: Dict) -> str:
        if sub["type"] == "user":
            return f"用户 {sub['username']}"
        elif sub["type"] == "repo":
            events = ", ".join(sub.get("events", ["commits"]))
            return f"仓库 {sub['repo']} 的 {events}"
        else:
            return f"项目 {sub['org']}/{sub['number']}"

    @filter.command("gh")
    async def gh(self, event: AstrMessageEvent):
        full_text = event.message_str.strip()
        parts = full_text.split()
        if len(parts) < 2:
            yield event.plain_result("请提供子命令，例如：gh subscribe octocat。输入 'gh help' 查看帮助。")
            return

        subcmd = parts[1].lower()
        args = parts[2:] if len(parts) > 2 else []

        if subcmd in ["subscribe", "sub"]:
            if not args:
                yield event.plain_result("请提供要订阅的目标。输入 'gh help' 查看格式。")
                return
            target = " ".join(args)
            async for result in self._cmd_subscribe(event, target):
                yield result
        elif subcmd in ["unsubscribe", "unsub"]:
            index = args[0] if args else None
            async for result in self._cmd_unsubscribe(event, index):
                yield result
        elif subcmd in ["list", "ls"]:
            async for result in self._cmd_list(event):
                yield result
        elif subcmd in ["pushnow", "push"]:
            async for result in self._cmd_pushnow(event):
                yield result
        elif subcmd in ["check", "chk"]:
            if not args:
                yield event.plain_result("请提供要查询的目标。输入 'gh help' 查看格式。")
                return
            target = " ".join(args)
            async for result in self._cmd_check(event, target):
                yield result
        elif subcmd in ["help", "h"]:
            async for result in self._cmd_help(event):
                yield result
        else:
            yield event.plain_result(f"未知子命令: {subcmd}。输入 'gh help' 查看帮助。")