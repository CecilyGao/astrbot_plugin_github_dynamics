import asyncio
import re
import os
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
SUBS_FILE = "subscriptions.json"

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

# ------------------------------- 主插件类 ------------------------------
@register(
    "astrbot_plugin_github_dynamics",
    "CecilyGao",
    "通过 GitHub API 监听用户动态、仓库(Issues/Commits/Releases)及组织项目动态，支持私有仓库，多会话独立订阅与定时推送",
    "0.0.1",
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

        # 用户名 -> QQ 映射
        self.username_qq_map: Dict[str, str] = self._load_username_qq_map()

        # 数据目录
        self.data_dir = StarTools.get_data_dir("astrbot_plugin_github_dynamics")
        os.makedirs(self.data_dir, exist_ok=True)
        self.subs_file = os.path.join(self.data_dir, SUBS_FILE)

        # 订阅数据结构：{ session_origin: [ {type, ...}, ... ] }
        self.subscriptions: Dict[str, List[Dict]] = {}

        self._poll_task: Optional[asyncio.Task] = None
        self._http_session: Optional[aiohttp.ClientSession] = None
        self._load_subscriptions()

    # ========================= 数据持久化 =========================
    def _load_subscriptions(self):
        if not os.path.exists(self.subs_file):
            self.subscriptions = {}
            return
        try:
            with open(self.subs_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                self.subscriptions = {}
            else:
                self.subscriptions = data
            logger.info(f"[GitHubDynamics] 已加载 {len(self.subscriptions)} 个会话的订阅")
        except Exception as e:
            logger.error(f"[GitHubDynamics] 加载订阅数据失败: {e}")
            self.subscriptions = {}

    def _save_subscriptions(self):
        try:
            to_save = {}
            for session, items in self.subscriptions.items():
                to_save[session] = []
                for item in items:
                    copy_item = item.copy()
                    copy_item.pop("cursor", None)
                    to_save[session].append(copy_item)
            with open(self.subs_file, "w", encoding="utf-8") as f:
                json.dump(to_save, f, ensure_ascii=False, indent=2)
            logger.debug("[GitHubDynamics] 订阅数据已保存")
        except Exception as e:
            logger.error(f"[GitHubDynamics] 保存订阅数据失败: {e}")

    def _get_cursor_key(self, session: str, sub: Dict) -> str:
        if sub["type"] == "user":
            return f"{KV_LAST_CURSOR_PREFIX}{session}_user_{sub['username']}"
        elif sub["type"] == "repo":
            return f"{KV_LAST_CURSOR_PREFIX}{session}_{sub['repo']}_{sub['event']}"
        else:  # project
            return f"{KV_LAST_CURSOR_PREFIX}{session}_project_{sub['org']}_{sub['number']}"

    async def _get_cursor(self, session: str, sub: Dict) -> str:
        key = self._get_cursor_key(session, sub)
        return await self.get_kv_data(key, "")

    async def _set_cursor(self, session: str, sub: Dict, cursor: str):
        key = self._get_cursor_key(session, sub)
        await self.put_kv_data(key, cursor)

    async def _init_subscription_cursor(self, session: str, sub: Dict):
        try:
            if sub["type"] == "user":
                latest = await self._fetch_latest_user_event(sub["username"])
            elif sub["type"] == "repo":
                latest = await self._fetch_latest_repo_entry(sub["repo"], sub["event"])
            else:
                latest = await self._fetch_latest_project_item(sub["org"], sub["number"])

            if latest:
                cursor = self._extract_cursor_from_entry(latest, sub["type"])
                if cursor:
                    await self._set_cursor(session, sub, cursor)
                    logger.info(f"[GitHubDynamics] 初始化游标成功: {self._get_cursor_key(session, sub)} -> {cursor[:20]}")
                    return
            await self._set_cursor(session, sub, "__EMPTY__")
            logger.warning(f"[GitHubDynamics] 无法获取最新条目，设置占位游标: {self._get_cursor_key(session, sub)}")
        except Exception as e:
            logger.error(f"[GitHubDynamics] 初始化游标失败: {e}")

    # ========================= 生命周期 =========================
    async def initialize(self):
        if not self.github_token:
            logger.error("[GitHubDynamics] github_token 未配置，插件将无法正常工作")
        logger.info(
            f"[GitHubDynamics] 初始化完成，轮询间隔: {self.poll_interval} 秒，"
            f"已加载 {len(self.subscriptions)} 个会话订阅"
        )
        self._http_session = aiohttp.ClientSession(
            headers={"Authorization": f"token {self.github_token}", "Accept": "application/vnd.github.v3+json"},
            timeout=aiohttp.ClientTimeout(total=30)
        )
        await self._ensure_all_cursors()
        self._poll_task = asyncio.create_task(self._poll_loop())

    async def _ensure_all_cursors(self):
        for session, items in self.subscriptions.items():
            for sub in items:
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
        if sub_type == "user":
            return str(entry.get("id", ""))
        elif sub_type in ("issue", "issues", "repo"):
            return str(entry.get("id", ""))
        elif sub_type in ("commit", "commits"):
            return entry.get("sha", "")
        elif sub_type in ("release", "releases"):
            return str(entry.get("id", ""))
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

    def _load_username_qq_map(self) -> Dict[str, str]:
        raw = self.config.get("username_qq", []) or []
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                pass
        mapping = {}
        if isinstance(raw, dict):
            for k, v in raw.items():
                mapping[str(k).strip().lower()] = str(v).strip()
        elif isinstance(raw, list):
            for item in raw:
                if isinstance(item, dict):
                    username = item.get("username") or item.get("login")
                    qq = item.get("qq")
                    if username and qq:
                        mapping[str(username).strip().lower()] = str(qq).strip()
        return mapping

    def _resolve_qq_for_username(self, username: str) -> Optional[str]:
        if not username:
            return None
        return self.username_qq_map.get(username.strip().lower())

    # ========================= GitHub API 请求 =========================
    async def _rest_api_get(self, url: str) -> Optional[List[Dict]]:
        if not self._http_session or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                headers={"Authorization": f"token {self.github_token}"},
                timeout=aiohttp.ClientTimeout(total=30)
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
                if isinstance(data, list):
                    return data
                else:
                    return [data]
        except Exception as e:
            logger.error(f"[GitHubDynamics] API 请求异常: {url} -> {e}")
            return None

    async def _graphql_request(self, query: str, variables: Dict = None) -> Optional[Dict]:
        if not self._http_session or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                headers={"Authorization": f"token {self.github_token}"},
                timeout=aiohttp.ClientTimeout(total=30)
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

    # ========================= 用户动态监听 =========================
    async def _fetch_user_events(self, username: str, per_page: int = 30) -> Optional[List[Dict]]:
        url = f"{REST_API_BASE}/users/{username}/events?per_page={per_page}"
        return await self._rest_api_get(url)

    async def _fetch_latest_user_event(self, username: str) -> Optional[Dict]:
        events = await self._fetch_user_events(username, per_page=1)
        if events and len(events) > 0:
            return events[0]
        return None

    async def _fetch_new_user_entries(self, username: str, last_cursor: str) -> Tuple[List[Dict], str]:
        per_page = MAX_SCAN_ENTRIES
        events = await self._fetch_user_events(username, per_page=per_page)
        if not events:
            return [], last_cursor

        new_entries = []
        for event in events:
            event_id = str(event.get("id", ""))
            if event_id == last_cursor:
                break
            entry = self._build_user_entry_dict(event)
            if entry:
                new_entries.append(entry)

        if self.max_entries > 0 and len(new_entries) > self.max_entries:
            new_entries = new_entries[:self.max_entries]

        if new_entries and events:
            latest_cursor = str(events[0].get("id", ""))
            return new_entries, latest_cursor
        return new_entries, last_cursor

    def _build_user_entry_dict(self, event: Dict) -> Dict:
        event_type = event.get("type", "Unknown")
        repo = event.get("repo", {}).get("name", "unknown/repo")
        created_at = event.get("created_at", "")
        payload = event.get("payload", {})

        title = f"[{event_type}] {repo}"
        link = f"https://github.com/{repo}"
        content = ""

        if event_type == "PushEvent":
            commits = payload.get("commits", [])
            cnt = len(commits)
            content = f"推送了 {cnt} 个提交"
        elif event_type == "IssuesEvent":
            action = payload.get("action", "")
            issue = payload.get("issue", {})
            number = issue.get("number", "")
            content = f"{action} issue #{number}: {issue.get('title', '')[:80]}"
            link = issue.get("html_url", link)
        elif event_type == "PullRequestEvent":
            action = payload.get("action", "")
            pr = payload.get("pull_request", {})
            number = pr.get("number", "")
            content = f"{action} PR #{number}: {pr.get('title', '')[:80]}"
            link = pr.get("html_url", link)
        elif event_type == "CreateEvent":
            ref_type = payload.get("ref_type", "")
            ref = payload.get("ref", "")
            content = f"创建了 {ref_type} {ref}".strip()
        elif event_type == "WatchEvent":
            content = "Star 了仓库"
        elif event_type == "ForkEvent":
            forkee = payload.get("forkee", {})
            content = f"Fork 了仓库到 {forkee.get('full_name', '')}"
            link = forkee.get("html_url", link)
        else:
            content = f"触发了 {event_type} 事件"

        return {
            "title": title,
            "link": link,
            "published": self._convert_time(created_at),
            "content": content,
            "id": str(event.get("id", "")),
            "type": "user_event",
            "author": username,
        }

    # ========================= 仓库动态 (Issues/Commits/Releases) =========================
    async def _fetch_latest_repo_entry(self, repo: str, event_type: str) -> Optional[Dict]:
        url = self._build_repo_api_url(repo, event_type, per_page=1)
        data = await self._rest_api_get(url)
        if data and len(data) > 0:
            return data[0]
        return None

    async def _fetch_new_repo_entries(self, repo: str, event_type: str, last_cursor: str) -> Tuple[List[Dict], str]:
        per_page = MAX_SCAN_ENTRIES
        url = self._build_repo_api_url(repo, event_type, per_page=per_page)
        items = await self._rest_api_get(url)
        if not items:
            return [], last_cursor

        new_entries = []
        for item in items:
            cursor = self._extract_cursor_from_entry(item, event_type)
            if cursor == last_cursor:
                break
            entry = self._build_repo_entry_dict(item, event_type)
            if entry:
                new_entries.append(entry)

        if self.max_entries > 0 and len(new_entries) > self.max_entries:
            new_entries = new_entries[:self.max_entries]

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

    def _build_repo_entry_dict(self, raw: Dict, event_type: str) -> Dict:
        if event_type == "issues":
            user = raw.get("user", {})
            return {
                "title": f"[Issue] #{raw['number']}: {raw['title']}",
                "link": raw["html_url"],
                "published": self._convert_time(raw["created_at"]),
                "content": (raw.get("body") or "")[:200],
                "id": str(raw["id"]),
                "type": "issue",
                "author": user.get("login", ""),
            }
        elif event_type == "commits":
            commit = raw.get("commit", {})
            return {
                "title": f"[Commit] {raw['sha'][:7]}: {commit.get('message', '').splitlines()[0][:100]}",
                "link": raw["html_url"],
                "published": self._convert_time(commit.get("committer", {}).get("date", "")),
                "content": commit.get("message", "").replace("\n", " ")[:200],
                "id": raw["sha"],
                "type": "commit",
            }
        elif event_type == "releases":
            return {
                "title": f"[Release] {raw['tag_name']}: {raw.get('name') or raw['tag_name']}",
                "link": raw["html_url"],
                "published": self._convert_time(raw.get("published_at") or raw["created_at"]),
                "content": (raw.get("body") or "")[:200],
                "id": str(raw["id"]),
                "type": "release",
            }
        return {}

    # ========================= 组织项目监听 (GraphQL) =========================
    async def _fetch_latest_project_item(self, org: str, number: int) -> Optional[Dict]:
        items = await self._fetch_project_items(org, number, first=1)
        if items:
            return items[0]
        return None

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
                                    author { login name }
                                }
                                ... on PullRequest {
                                    id number title url bodyText state merged mergedAt
                                    createdAt updatedAt
                                    author { login name }
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
            entry = self._build_project_entry_dict(item, org, number)
            if not entry:
                continue
            entry_time = entry.get("raw_updated_at", "")
            if entry_time > last_cursor:
                new_entries.append(entry)
                if entry_time > max_time:
                    max_time = entry_time

        if self.max_entries > 0 and len(new_entries) > self.max_entries:
            new_entries = new_entries[-self.max_entries:]

        return new_entries, max_time

    def _build_project_entry_dict(self, item: Dict, org: str, number: int) -> Dict:
        content = item.get("content")
        if not content:
            return {}
        typename = content.get("__typename", "Card")
        title = content.get("title", "无标题")
        url = content.get("url", "")
        updated_at = content.get("updatedAt") or item.get("updatedAt") or ""
        return {
            "title": f"[{typename}] {title}",
            "link": url,
            "published": self._convert_time(updated_at),
            "raw_updated_at": updated_at,
            "id": item.get("id", ""),
            "type": "project_item",
            "content": (content.get("bodyText") or "")[:200],
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

        messages_by_session: Dict[str, List[Tuple[str, List[str]]]] = {}

        for session, items in list(self.subscriptions.items()):
            for sub in items:
                try:
                    last_cursor = await self._get_cursor(session, sub)
                    if not last_cursor:
                        await self._init_subscription_cursor(session, sub)
                        continue
                    if last_cursor == "__EMPTY__":
                        continue

                    if sub["type"] == "user":
                        new_entries, new_cursor = await self._fetch_new_user_entries(sub["username"], last_cursor)
                    elif sub["type"] == "repo":
                        new_entries, new_cursor = await self._fetch_new_repo_entries(sub["repo"], sub["event"], last_cursor)
                    else:  # project
                        new_entries, new_cursor = await self._fetch_new_project_entries(sub["org"], sub["number"], last_cursor)

                    if new_entries:
                        if sub["type"] == "user":
                            msg = self._format_user_entries(sub["username"], new_entries)
                        elif sub["type"] == "repo":
                            msg = self._format_repo_entries(sub["repo"], sub["event"], new_entries)
                        else:
                            msg = self._format_project_entries(f"{sub['org']}/{sub['number']}", new_entries)

                        if msg:
                            assignees = []
                            for e in new_entries:
                                if e.get("author"):
                                    assignees.append(e["author"])
                            messages_by_session.setdefault(session, []).append((msg, assignees))

                    if new_cursor and new_cursor != last_cursor:
                        await self._set_cursor(session, sub, new_cursor)
                except Exception as e:
                    logger.error(f"[GitHubDynamics] 处理订阅 {sub} 失败: {e}")

        for session, msg_list in messages_by_session.items():
            full_msg = "\n\n".join([m[0] for m in msg_list])
            chain = MessageChain().message(full_msg)
            if self.at_enable:
                ats = set()
                for _, ass in msg_list:
                    for a in ass:
                        qq = self._resolve_qq_for_username(a)
                        if qq:
                            ats.add((a, qq))
                for name, qq in ats:
                    try:
                        chain.at(name, str(qq))
                        chain.message(" ")
                    except Exception:
                        pass
            try:
                await self.context.send_message(session, chain)
            except Exception as e:
                logger.error(f"[GitHubDynamics] 推送到 {session} 失败: {e}")

    # ========================= 消息格式化 =========================
    @staticmethod
    def _format_user_entries(username: str, entries: List[Dict]) -> str:
        lines = [f"👤 用户 {username} 的新动态（{len(entries)} 条）：\n"]
        for i, e in enumerate(entries, 1):
            lines.append(f"  {i}. {e['title']}")
            if e.get("published"):
                lines.append(f"     🕐 时间: {e['published']}")
            if e.get("content"):
                lines.append(f"     📝 {e['content']}")
            if e.get("link"):
                lines.append(f"     🔗 {e['link']}")
            lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _format_repo_entries(repo: str, event_type: str, entries: List[Dict]) -> str:
        icon = {"issues": "🐛", "commits": "📝", "releases": "📦"}.get(event_type, "🔔")
        lines = [f"{icon} 仓库 {repo} 的新 {event_type} 动态（{len(entries)} 条）：\n"]
        for i, e in enumerate(entries, 1):
            lines.append(f"  {i}. {e['title']}")
            if e.get("published"):
                lines.append(f"     🕐 时间: {e['published']}")
            if e.get("content"):
                lines.append(f"     📝 {e['content']}")
            if e.get("author"):
                lines.append(f"     🙋 提出者: @{e['author']}")
            if e.get("link"):
                lines.append(f"     🔗 {e['link']}")
            lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _format_project_entries(project_id: str, entries: List[Dict]) -> str:
        lines = [f"📢 组织项目 {project_id} 有 {len(entries)} 个新动态：\n"]
        for i, e in enumerate(entries, 1):
            lines.append(f"  {i}. {e['title']}")
            if e.get("published"):
                lines.append(f"     🕐 时间: {e['published']}")
            if e.get("content"):
                lines.append(f"     📝 {e['content']}")
            if e.get("link"):
                lines.append(f"     🔗 {e['link']}")
            lines.append("")
        return "\n".join(lines)

    # ========================= 订阅管理指令 =========================
    @filter.command("gh subscribe")
    async def gh_subscribe(self, event: AstrMessageEvent):
        """在当前会话订阅一个监听目标
        用法:
          gh subscribe <用户名>
          gh subscribe <仓库名>[:issues|commits|releases]   # 默认 commits
          gh subscribe <组织名>/<项目编号>
        """
        if not is_user_allowed(self, event):
            yield event.plain_result("❌ 你没有权限使用此指令")
            return
        parts = event.message_str.strip().split(maxsplit=2)
        if len(parts) < 2:
            yield event.plain_result("❌ 用法错误，请提供监听目标")
            return
        target = parts[1].strip()
        session = get_session_id(event)

        # 判断类型
        if RE_PROJECT_ITEM.match(target):
            match = RE_PROJECT_ITEM.match(target)
            org, num = match.group(1), int(match.group(2))
            new_sub = {"type": "project", "org": org, "number": num}
            display = f"项目 {org}/{num}"
        elif "/" in target:
            # 仓库可能带事件后缀
            repo_part = target
            event_type = "commits"
            if ":" in target:
                repo_part, event_type = target.split(":", 1)
                if event_type not in ("issues", "commits", "releases"):
                    yield event.plain_result("❌ 事件类型必须为 issues / commits / releases")
                    return
            if not RE_REPO.match(repo_part):
                yield event.plain_result("❌ 仓库格式不正确，应为 owner/repo")
                return
            new_sub = {"type": "repo", "repo": repo_part, "event": event_type}
            display = f"仓库 {repo_part} 的 {event_type}"
        else:
            if not RE_USER.match(target):
                yield event.plain_result("❌ 用户名格式不正确")
                return
            new_sub = {"type": "user", "username": target}
            display = f"用户 {target}"

        # 检查重复
        for sub in self.subscriptions.get(session, []):
            if sub == new_sub:
                yield event.plain_result(f"⚠️ 当前会话已订阅 {display}")
                return

        if session not in self.subscriptions:
            self.subscriptions[session] = []
        self.subscriptions[session].append(new_sub)
        self._save_subscriptions()
        await self._init_subscription_cursor(session, new_sub)
        yield event.plain_result(f"✅ 已成功订阅：{display}")

    @filter.command("gh unsubscribe")
    async def gh_unsubscribe(self, event: AstrMessageEvent, index: str = None):
        """取消订阅，序号通过 gh list 查看"""
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
            self._save_subscriptions()
            key = self._get_cursor_key(session, removed)
            await self.put_kv_data(key, "")
            yield event.plain_result(f"✅ 已取消订阅：{self._format_sub(removed)}")
        except ValueError:
            yield event.plain_result("❌ 序号必须为数字")

    @filter.command("gh list")
    async def gh_list(self, event: AstrMessageEvent):
        """列出当前会话的所有订阅"""
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

    def _format_sub(self, sub: Dict) -> str:
        if sub["type"] == "user":
            return f"用户 {sub['username']}"
        elif sub["type"] == "repo":
            return f"仓库 {sub['repo']} 的 {sub['event']}"
        else:
            return f"项目 {sub['org']}/{sub['number']}"

    @filter.command("gh pushnow")
    async def gh_pushnow(self, event: AstrMessageEvent):
        """立即推送当前会话所有订阅的最新动态"""
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
        yield event.plain_result("🔄 正在检查并推送订阅动态...")
        await self._do_push_for_session(session)
        yield event.plain_result("✅ 推送完成（如有新动态已发送）")

    async def _do_push_for_session(self, session: str):
        """只检查指定会话的订阅并推送"""
        items = self.subscriptions.get(session, [])
        messages = []
        for sub in items:
            try:
                last_cursor = await self._get_cursor(session, sub)
                if not last_cursor or last_cursor == "__EMPTY__":
                    continue
                if sub["type"] == "user":
                    new_entries, new_cursor = await self._fetch_new_user_entries(sub["username"], last_cursor)
                elif sub["type"] == "repo":
                    new_entries, new_cursor = await self._fetch_new_repo_entries(sub["repo"], sub["event"], last_cursor)
                else:
                    new_entries, new_cursor = await self._fetch_new_project_entries(sub["org"], sub["number"], last_cursor)

                if new_entries:
                    if sub["type"] == "user":
                        msg = self._format_user_entries(sub["username"], new_entries)
                    elif sub["type"] == "repo":
                        msg = self._format_repo_entries(sub["repo"], sub["event"], new_entries)
                    else:
                        msg = self._format_project_entries(f"{sub['org']}/{sub['number']}", new_entries)
                    if msg:
                        messages.append(msg)
                if new_cursor and new_cursor != last_cursor:
                    await self._set_cursor(session, sub, new_cursor)
            except Exception as e:
                logger.error(f"[GitHubDynamics] 推送会话 {session} 订阅 {sub} 失败: {e}")
        if messages:
            full_msg = "\n\n".join(messages)
            chain = MessageChain().message(full_msg)
            try:
                await self.context.send_message(session, chain)
            except Exception as e:
                logger.error(f"[GitHubDynamics] 推送到 {session} 失败: {e}")

    @filter.command("gh check")
    async def gh_check(self, event: AstrMessageEvent):
        """立刻查询指定目标的最新动态（不添加订阅）
        用法: 同 gh subscribe
        """
        if not is_user_allowed(self, event):
            yield event.plain_result("❌ 你没有权限使用此指令")
            return
        parts = event.message_str.strip().split(maxsplit=2)
        if len(parts) < 2:
            yield event.plain_result("❌ 用法错误，请提供查询目标")
            return
        target = parts[1].strip()

        if RE_PROJECT_ITEM.match(target):
            match = RE_PROJECT_ITEM.match(target)
            org, num = match.group(1), int(match.group(2))
            yield event.plain_result(f"🔄 正在查询项目 {org}/{num} ...")
            items = await self._fetch_project_items(org, num, first=self.max_entries or 10)
            if not items:
                yield event.plain_result("❌ 无法获取项目数据，请检查组织名、项目编号及 token 权限")
                return
            entries = []
            for item in items[:self.max_entries or 10]:
                entry = self._build_project_entry_dict(item, org, num)
                if entry:
                    entries.append(entry)
            if entries:
                yield event.plain_result(self._format_project_entries(f"{org}/{num}", entries))
            else:
                yield event.plain_result(f"🔍 项目 {org}/{num} 暂无最近动态。")
            return

        if "/" in target:
            repo_part = target
            event_type = "commits"
            if ":" in target:
                repo_part, event_type = target.split(":", 1)
                if event_type not in ("issues", "commits", "releases"):
                    yield event.plain_result("❌ 事件类型必须为 issues / commits / releases")
                    return
            if not RE_REPO.match(repo_part):
                yield event.plain_result("❌ 仓库格式不正确，应为 owner/repo")
                return
            yield event.plain_result(f"🔄 正在查询仓库 {repo_part} 的 {event_type} ...")
            url = self._build_repo_api_url(repo_part, event_type, per_page=self.max_entries or 10)
            items = await self._rest_api_get(url)
            if not items:
                yield event.plain_result("❌ 无法获取数据，请检查仓库名及 token 权限")
                return
            entries = []
            for item in items[:self.max_entries or 10]:
                entry = self._build_repo_entry_dict(item, event_type)
                if entry:
                    entries.append(entry)
            if entries:
                yield event.plain_result(self._format_repo_entries(repo_part, event_type, entries))
            else:
                yield event.plain_result(f"🔍 仓库 {repo_part} 暂无最近的 {event_type} 动态。")
            return

        # 用户
        if not RE_USER.match(target):
            yield event.plain_result("❌ 用户名格式不正确")
            return
        yield event.plain_result(f"🔄 正在查询用户 {target} 的最新动态...")
        events = await self._fetch_user_events(target, per_page=self.max_entries or 10)
        if not events:
            yield event.plain_result(f"🔍 用户 {target} 暂无最近公开动态。")
            return
        entries = []
        for ev in events[:self.max_entries or 10]:
            entry = self._build_user_entry_dict(ev)
            if entry:
                entries.append(entry)
        if entries:
            yield event.plain_result(self._format_user_entries(target, entries))
        else:
            yield event.plain_result(f"🔍 用户 {target} 暂无最近动态。")

    @filter.command("gh here")
    async def gh_here(self, event: AstrMessageEvent):
        """显示当前会话 ID 及订阅概况"""
        session = get_session_id(event)
        items = self.subscriptions.get(session, [])
        if items:
            msg = f"📌 当前会话已绑定 {len(items)} 个订阅。\n"
            for i, sub in enumerate(items, 1):
                msg += f"{i}. {self._format_sub(sub)}\n"
            msg += "\n💡 使用 gh subscribe 添加订阅，gh unsubscribe 取消订阅。"
        else:
            msg = f"📌 当前会话 ID：`{session}`\n尚未订阅任何目标。使用 `gh subscribe` 添加监听。"
        yield event.plain_result(msg)
