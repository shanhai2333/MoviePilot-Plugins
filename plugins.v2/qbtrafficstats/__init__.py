"""
QB 流量统计（MoviePilot v2 插件）。

把 qBittorrent 的上传 / 下载流量按「当前累计、较上次新增、今日新增、本月累计」
推送到 MoviePilot 的通知渠道。设计上主要给外部 cron 调用 HTTP 接口触发：

    curl "http://<MoviePilot地址>:3001/api/v1/plugin/QbTrafficStats/push"

关于计数口径与「不能为负」：

- 优先用 qB 的**全时计数**（sync/maindata → server_state.alltime_dl / alltime_ul）。
  它由 qB 自己写在 qBittorrent-data.ini 的 Stats/AllStats 里，重启后仍然保留
  （qB 默认每 15 分钟落盘一次，正常退出也会落盘）。
- 拿不到全时计数时退化为**会话计数**（transfer/info → dl_info_data / up_info_data），
  它每次 qB 重启都会清零，是 qB 源码里注释的 "Data downloaded this session"。
- 两种口径都做回退检测：只要当前值比上次记录的小，就判定为计数器回退，
  全时口径按 0 计（丢失的区间无法还原），会话口径按当前值计（等于重启后新产生的量）。
  任何情况下增量都被钳到 >= 0，不会出现负数。
- 今日 / 本月累计由本插件按增量自己累加，跨天、跨月自动重新起算。
"""

import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.triggers.cron import CronTrigger
from app.core.config import settings
from app.core.event import Event, eventmanager
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType, ServiceInfo
from app.schemas.types import EventType


class QbTrafficStats(_PluginBase):
    # 插件名称
    plugin_name = "QB流量统计"
    # 插件描述
    plugin_desc = (
        "定时推送 qBittorrent 的上传/下载流量：当前累计、较上次新增、今日新增、本月累计。"
        "填一个 cron 表达式即可，另可选提供 HTTP 接口给外部调度器调用；"
        "基于 qB 全时计数统计，qB 重启或计数回退时自动兜底，不会出现负数。"
    )
    # 插件图标
    plugin_icon = "Qbittorrent_A.png"
    # 插件版本
    plugin_version = "1.1"
    # 插件作者
    plugin_author = "shanhai2333"
    # 作者主页
    author_url = "https://github.com/shanhai2333"
    # 插件配置项ID前缀
    plugin_config_prefix = "qbtrafficstats_"
    # 加载顺序
    plugin_order = 23
    # 可使用的用户级别
    auth_level = 1

    # 日志前缀
    LOG_TAG = "[QB流量统计]"
    # 数据源标识
    SOURCE_ALLTIME = "alltime"
    SOURCE_SESSION = "session"
    # 接口路径（MoviePilot 会自动在前面拼 /api/v1/plugin/<类名>）
    API_PATH = "/push"
    # 日期格式
    TIME_FMT = "%Y-%m-%d %H:%M:%S"

    # region 私有属性
    # 是否启用
    _enabled: bool = False
    # 选中的下载器名称
    _downloaders: List[str] = []
    # 定时推送的 cron 表达式（留空则不定时，只响应命令/接口）
    _cron: str = ""
    # 接口调用令牌（留空则不校验）
    _api_token: str = ""
    # 各下载器的统计状态 {下载器名: {...}}
    _state: Dict[str, Dict[str, Any]] = {}
    # 并发锁
    _lock = threading.Lock()

    # endregion

    def init_plugin(self, config: dict = None):
        """
        插件初始化
        """
        self._enabled = False
        self._state = {}

        if config:
            self._enabled = bool(config.get("enabled"))
            self._downloaders = self.__to_list(config.get("downloaders"))
            self._cron = str(config.get("cron") or "").strip()
            self._api_token = str(config.get("api_token") or "").strip()

        # 恢复跨会话的统计基准，避免插件重启后把「上次新增」算错
        saved = self.get_data("state")
        if isinstance(saved, dict):
            self._state = saved

        logger.info(
            f"{self.LOG_TAG}配置加载：enabled={self._enabled}, "
            f"下载器={self._downloaders or '未选择'}, "
            f"定时={'未设置' if not self._cron else self._cron}, "
            f"接口令牌={'已设置' if self._api_token else '未设置'}, "
            f"已记录 {len(self._state)} 个下载器的统计基准"
        )

        # 定时表达式校验：写错了直接告诉用户，别等到调度器报错
        if self._cron and self._enabled and not self.__parse_cron(self._cron):
            logger.error(
                f"{self.LOG_TAG}定时表达式「{self._cron}」不合法，定时推送不会生效。"
                f"示例：每小时 0 * * * *，每天 8 点 0 8 * * *，每 6 小时 0 */6 * * *"
            )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        定义远程控制命令
        """
        return [
            {
                "cmd": "/qb_traffic_push",
                "event": EventType.PluginAction,
                "desc": "立即推送一次QB流量统计",
                "category": "QB",
                "data": {"action": "qb_traffic_push"},
            },
            {
                "cmd": "/qb_traffic_reset",
                "event": EventType.PluginAction,
                "desc": "重置QB流量统计的累计基准",
                "category": "QB",
                "data": {"action": "qb_traffic_reset"},
            },
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """
        注册插件API，供外部 cron 调用
        """
        return [
            {
                "path": self.API_PATH,
                "endpoint": self.api_push,
                "methods": ["GET"],
                "summary": "推送QB流量统计",
                "description": (
                    "读取已选下载器的上传/下载流量并推送通知，同时返回本次统计的 JSON。"
                    "给外部调度器（群晖计划任务、青龙等）调用；"
                    "如果只想定时推送，直接在插件配置里填 cron 表达式即可，不需要用这个接口。"
                ),
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务：按 cron 表达式定时推送
        """
        if not self._enabled or not self._cron:
            return []

        trigger = self.__parse_cron(self._cron)
        if not trigger:
            logger.error(f"{self.LOG_TAG}定时表达式「{self._cron}」不合法，本次不注册定时任务")
            return []

        return [
            {
                "id": "QbTrafficStats",
                "name": "QB流量统计定时推送",
                "trigger": trigger,
                "func": self.push_stats,
                "kwargs": {},
            }
        ]

    def stop_service(self):
        """
        退出插件
        """
        logger.info(f"{self.LOG_TAG}插件已停止（已保存的统计基准会保留）")

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面
        """
        downloader_items = [
            {"title": config.name, "value": config.name}
            for config in DownloaderHelper().get_configs().values()
        ]

        token_hint = (
            f"只有用外部调度器调接口时才需要填。接口地址：{self.__api_url()}"
            "，填写后调用需带上 ?token=xxx，留空则不校验。"
            "插件接口不需要 MoviePilot 登录，建议只在内网使用，或在这里设个令牌。"
        )

        cron_hint = (
            "5 段式，分 时 日 月 周。示例：每小时 0 * * * *；每天 8 点 0 8 * * *；"
            "每 6 小时 0 */6 * * *；每周一 9 点 0 9 * * 1。留空则不定时推送。"
        )

        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 9},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cron",
                                            "label": "定时推送周期（cron 表达式）",
                                            "placeholder": "0 * * * *",
                                            "hint": cron_hint,
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                            "model": "downloaders",
                                            "label": "下载器",
                                            "items": downloader_items,
                                            "hint": "选择需要统计的 qBittorrent 下载器",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "api_token",
                                            "label": "接口令牌（可选）",
                                            "hint": token_hint,
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": (
                                                "定时推送由 MoviePilot 自己的调度器执行，"
                                                "填好 cron 表达式保存即可，不需要另配外部 crontab。"
                                                "「较上次新增」是距上一次推送之间的增量，"
                                                "所以周期越长，这个数字覆盖的时间跨度越大。"
                                                "想立刻看一次效果，发远程命令 /qb_traffic_push 即可。"
                                            ),
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "warning",
                                            "variant": "tonal",
                                            "text": (
                                                "关于计数口径：优先使用 qB 的全时计数"
                                                "（alltime_dl / alltime_ul，保存在 qB 配置里，重启不丢）；"
                                                "读不到时退化为会话计数（qB 重启会清零）。"
                                                "两种口径都做了回退检测，计数器变小就按 0 计或按当前值计，"
                                                "不会出现负数。「今日新增」「本月累计」由本插件按增量累加，"
                                                "跨天、跨月自动重新起算；跨零点的那一轮增量会整段计入新的一天，"
                                                "所以建议配合每小时级别的 cron 使用，间隔越长这一段的归属越粗。"
                                                "重置基准可用远程命令 /qb_traffic_reset。"
                                            ),
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ]
            }
        ], {
            "enabled": False,
            "cron": "0 * * * *",
            "downloaders": [],
            "api_token": "",
        }

    def get_page(self) -> List[dict]:
        """
        插件详情页
        """
        state = self._state or {}
        if not state:
            return [
                {
                    "component": "VAlert",
                    "props": {
                        "type": "info",
                        "variant": "tonal",
                        "text": "暂无统计数据。插件启用后调用一次接口或发送 /qb_traffic_push 命令，这里会显示各下载器的流量。",
                    },
                }
            ]

        rows = []
        for name, item in state.items():
            rows.append(
                {
                    "component": "tr",
                    "content": [
                        {"component": "td", "text": name},
                        {"component": "td", "text": self.__source_label(item.get("source"))},
                        {"component": "td", "text": self.__fmt_pair(item.get("last_ul"), item.get("last_dl"))},
                        {"component": "td", "text": self.__fmt_pair(item.get("delta_ul"), item.get("delta_dl"))},
                        {"component": "td", "text": self.__fmt_pair(item.get("today_ul"), item.get("today_dl"))},
                        {"component": "td", "text": self.__fmt_pair(item.get("month_ul"), item.get("month_dl"))},
                        {"component": "td", "text": item.get("last_time") or "-"},
                    ],
                }
            )

        return [
            {
                "component": "VTable",
                "props": {"hover": True, "density": "comfortable"},
                "content": [
                    {
                        "component": "thead",
                        "content": [
                            {
                                "component": "tr",
                                "content": [
                                    {"component": "th", "text": "下载器"},
                                    {"component": "th", "text": "数据源"},
                                    {"component": "th", "text": "累计（上/下）"},
                                    {"component": "th", "text": "上次新增（上/下）"},
                                    {"component": "th", "text": "今日新增（上/下）"},
                                    {"component": "th", "text": "本月累计（上/下）"},
                                    {"component": "th", "text": "上次采集"},
                                ],
                            }
                        ],
                    },
                    {"component": "tbody", "content": rows},
                ],
            }
        ]

    @eventmanager.register(EventType.PluginAction)
    def handle_plugin_action(self, event: Event):
        """
        处理远程命令
        """
        if not event or not event.event_data:
            return

        action = event.event_data.get("action")
        if action == "qb_traffic_push":
            result = self.push_stats(manual=True)
            if not result.get("success"):
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【QB流量统计】",
                    text=result.get("message") or "推送失败",
                )
        elif action == "qb_traffic_reset":
            self.reset_stats()

    def api_push(self, token: str = None) -> Dict[str, Any]:
        """
        供外部 cron 调用的推送接口

        :param token: 接口令牌，配置了令牌时必须一致
        """
        if self._api_token and token != self._api_token:
            logger.warning(f"{self.LOG_TAG}接口令牌不正确，已拒绝本次调用")
            return {"success": False, "message": "接口令牌不正确"}
        if not self._enabled:
            return {"success": False, "message": "插件未启用，请先在插件页面启用并保存"}

        return self.push_stats(manual=True)

    def push_stats(self, manual: bool = False) -> Dict[str, Any]:
        """
        采集一次流量并推送
        """
        if not self._enabled and not manual:
            return {"success": False, "message": "插件未启用"}

        services = self.__get_services()
        if not services:
            return {"success": False, "message": "没有可用的 qBittorrent 下载器，请检查插件配置"}

        now = datetime.now(pytz.timezone(settings.TZ))
        records: List[Dict[str, Any]] = []
        with self._lock:
            for name, service in services.items():
                try:
                    record = self.__collect_one(name, service, now)
                except Exception as err:
                    logger.error(f"{self.LOG_TAG}统计下载器 [{name}] 出错：{err}", exc_info=True)
                    continue
                if record:
                    records.append(record)
            # 统一落盘，避免中途异常把状态写坏
            self.save_data("state", self._state)

        if not records:
            return {"success": False, "message": "本次没有采集到任何数据"}

        self.__notify(records, now)
        return {
            "success": True,
            "message": f"已推送 {len(records)} 个下载器的流量统计",
            "data": records,
        }

    def reset_stats(self):
        """
        清空统计基准（下次采集重新起算）
        """
        with self._lock:
            self._state = {}
            self.save_data("state", self._state)
        logger.info(f"{self.LOG_TAG}统计基准已重置")
        self.post_message(
            mtype=NotificationType.Plugin,
            title="【QB流量统计】",
            text="统计基准已重置，下次采集将重新起算「上次新增 / 今日新增 / 本月累计」。",
        )

    def __collect_one(self, name: str, service: ServiceInfo,
                      now: datetime) -> Optional[Dict[str, Any]]:
        """
        采集单个下载器的流量
        """
        qb = getattr(service, "instance", None)
        if not qb:
            logger.warning(f"{self.LOG_TAG}下载器 [{name}] 实例不存在，跳过")
            return None

        counters = self.__read_counters(qb)
        if not counters:
            logger.warning(f"{self.LOG_TAG}下载器 [{name}] 读取流量计数失败，跳过本轮（状态保持不变）")
            return None

        cur_dl, cur_ul, source = counters
        state = self._state.get(name) or {}
        first_run = "last_dl" not in state

        # 1) 计算相对上次的新增
        if first_run:
            delta_dl = 0
            delta_ul = 0
            note = "首次采集，已记录基准"
        else:
            delta_dl = cur_dl - self.__to_int(state.get("last_dl"))
            delta_ul = cur_ul - self.__to_int(state.get("last_ul"))
            note = ""
            prev_source = state.get("source")
            if prev_source and prev_source != source:
                # 全时计数偶发读取失败会退化到会话计数，两者数值不可比，
                # 直接比较会算出一个巨大的假增量，这里重新记录基准。
                delta_dl = 0
                delta_ul = 0
                note = (
                    f"数据源由「{self.__source_label(prev_source)}」切换为"
                    f"「{self.__source_label(source)}」，已重新记录基准"
                )
                logger.warning(f"{self.LOG_TAG}下载器 [{name}] {note}")
            elif delta_dl < 0 or delta_ul < 0:
                # 计数器回退：qB 重启或统计被重置
                if source == self.SOURCE_SESSION:
                    # 会话计数从 0 重新开始，当前值即重启后新产生的量
                    delta_dl = cur_dl
                    delta_ul = cur_ul
                    note = "检测到 qB 重启（会话计数清零），本轮按重启后累计计新增"
                else:
                    # 全时计数回退说明 qB 非正常退出，丢失的区间无法还原
                    delta_dl = 0
                    delta_ul = 0
                    note = "检测到 qB 统计回退（可能非正常退出），本轮按 0 计"
                logger.warning(f"{self.LOG_TAG}下载器 [{name}] {note}")

        # 无论如何都不允许出现负数
        delta_dl = max(delta_dl, 0)
        delta_ul = max(delta_ul, 0)

        # 2) 今日 / 本月累计：跨天、跨月重新起算
        today = now.strftime("%Y-%m-%d")
        month = now.strftime("%Y-%m")
        if state.get("day") == today:
            today_dl = self.__to_int(state.get("today_dl"))
            today_ul = self.__to_int(state.get("today_ul"))
        else:
            today_dl = 0
            today_ul = 0
        if state.get("month") == month:
            month_dl = self.__to_int(state.get("month_dl"))
            month_ul = self.__to_int(state.get("month_ul"))
        else:
            month_dl = 0
            month_ul = 0

        today_dl += delta_dl
        today_ul += delta_ul
        month_dl += delta_dl
        month_ul += delta_ul

        # 3) 保存状态
        self._state[name] = {
            "last_dl": cur_dl,
            "last_ul": cur_ul,
            "last_time": now.strftime(self.TIME_FMT),
            "source": source,
            "day": today,
            "today_dl": today_dl,
            "today_ul": today_ul,
            "month": month,
            "month_dl": month_dl,
            "month_ul": month_ul,
            "delta_dl": delta_dl,
            "delta_ul": delta_ul,
        }

        interval = "" if first_run else self.__fmt_interval(state.get("last_time"), now)
        logger.info(
            f"{self.LOG_TAG}[{name}] 数据源={self.__source_label(source)}, "
            f"累计 上传{self.__fmt_bytes(cur_ul)}/下载{self.__fmt_bytes(cur_dl)}, "
            f"本次新增 上传{self.__fmt_bytes(delta_ul)}/下载{self.__fmt_bytes(delta_dl)}, "
            f"今日 上传{self.__fmt_bytes(today_ul)}/下载{self.__fmt_bytes(today_dl)}, "
            f"本月 上传{self.__fmt_bytes(month_ul)}/下载{self.__fmt_bytes(month_dl)}"
        )

        return {
            "name": name,
            "source": source,
            "total_dl": cur_dl,
            "total_ul": cur_ul,
            "delta_dl": delta_dl,
            "delta_ul": delta_ul,
            "today_dl": today_dl,
            "today_ul": today_ul,
            "month_dl": month_dl,
            "month_ul": month_ul,
            "interval": interval,
            "note": note,
        }

    @classmethod
    def __read_counters(cls, qb) -> Optional[Tuple[int, int, str]]:
        """
        读取 (下载总量, 上传总量, 数据源)，单位字节

        优先全时计数（跨 qB 重启保留），拿不到时退化为会话计数。
        """
        alltime = cls.__read_alltime(qb)
        if alltime:
            return alltime[0], alltime[1], cls.SOURCE_ALLTIME

        session = cls.__read_session(qb)
        if session:
            return session[0], session[1], cls.SOURCE_SESSION

        return None

    @staticmethod
    def __read_alltime(qb) -> Optional[Tuple[int, int]]:
        """
        通过 sync/maindata 读取 qB 的全时上传 / 下载量

        对应 server_state 里的 alltime_dl / alltime_ul，
        由 qB 自己落盘在 qBittorrent-data.ini 的 Stats/AllStats，重启后仍在。
        """
        client = getattr(qb, "qbc", None)
        if client is None:
            return None

        try:
            data = client.sync_maindata(rid=0)
        except AttributeError:
            # 旧版 qbittorrent-api 没有该接口
            logger.debug(f"{QbTrafficStats.LOG_TAG}当前 qbittorrent-api 不支持 sync_maindata，改用会话计数")
            return None
        except Exception as err:
            logger.debug(f"{QbTrafficStats.LOG_TAG}读取全时统计失败：{err}")
            return None

        if not data or not hasattr(data, "get"):
            return None

        server_state = data.get("server_state")
        if not isinstance(server_state, dict):
            # qbittorrent-api 的 Dictionary 同时支持属性访问，兜一层
            server_state = getattr(data, "server_state", None)
        if not isinstance(server_state, dict):
            return None

        download = server_state.get("alltime_dl")
        upload = server_state.get("alltime_ul")
        if download is None or upload is None:
            return None

        try:
            return max(int(download), 0), max(int(upload), 0)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def __read_session(qb) -> Optional[Tuple[int, int]]:
        """
        读取 qB 的会话上传 / 下载量（qB 重启会清零）
        """
        try:
            info = qb.transfer_info()
        except Exception as err:
            logger.error(f"{QbTrafficStats.LOG_TAG}读取传输信息出错：{err}")
            return None

        if not info or not hasattr(info, "get"):
            return None

        download = info.get("dl_info_data")
        upload = info.get("up_info_data")
        if download is None or upload is None:
            return None

        try:
            return max(int(download), 0), max(int(upload), 0)
        except (TypeError, ValueError):
            return None

    def __get_services(self) -> Dict[str, ServiceInfo]:
        """
        获取并校验可用的 qBittorrent 下载器实例
        """
        if not self._downloaders:
            logger.warning(f"{self.LOG_TAG}尚未选择下载器")
            return {}

        helper = DownloaderHelper()
        services = helper.get_services(name_filters=self._downloaders)
        if not services:
            logger.warning(f"{self.LOG_TAG}获取下载器实例失败，请检查配置")
            return {}

        result: Dict[str, ServiceInfo] = {}
        for name, service in services.items():
            if not helper.is_downloader(service_type="qbittorrent", service=service):
                logger.warning(f"{self.LOG_TAG}下载器 [{name}] 不是 qBittorrent，已跳过")
                continue
            if not getattr(service, "instance", None):
                logger.warning(f"{self.LOG_TAG}下载器 [{name}] 实例不存在，已跳过")
                continue
            try:
                if service.instance.is_inactive():
                    logger.warning(f"{self.LOG_TAG}下载器 [{name}] 未连接，已跳过")
                    continue
            except Exception as err:
                logger.warning(f"{self.LOG_TAG}下载器 [{name}] 连接状态检查失败：{err}")
                continue
            result[name] = service
        return result

    def __notify(self, records: List[Dict[str, Any]], now: datetime):
        """
        推送流量统计通知
        """
        blocks = []
        for item in records:
            lines = [f"▎{item['name']}"]
            lines.append(
                f"当前累计：上传 {self.__fmt_bytes(item['total_ul'])}"
                f" / 下载 {self.__fmt_bytes(item['total_dl'])}"
            )
            delta_title = "较上次新增"
            if item.get("interval"):
                delta_title += f"（间隔 {item['interval']}）"
            lines.append(
                f"{delta_title}：上传 {self.__fmt_bytes(item['delta_ul'])}"
                f" / 下载 {self.__fmt_bytes(item['delta_dl'])}"
            )
            lines.append(
                f"今日新增：上传 {self.__fmt_bytes(item['today_ul'])}"
                f" / 下载 {self.__fmt_bytes(item['today_dl'])}"
            )
            lines.append(
                f"本月累计：上传 {self.__fmt_bytes(item['month_ul'])}"
                f" / 下载 {self.__fmt_bytes(item['month_dl'])}"
            )
            if item.get("note"):
                lines.append(f"注：{item['note']}")
            blocks.append("\n".join(lines))

        self.post_message(
            mtype=NotificationType.Plugin,
            title=f"【QB流量统计】{now.strftime('%m-%d %H:%M')}",
            text="\n\n".join(blocks),
        )

    @staticmethod
    def __parse_cron(expression: str) -> Optional[CronTrigger]:
        """
        解析 cron 表达式，非法时返回 None
        """
        if not expression:
            return None
        try:
            return CronTrigger.from_crontab(expression)
        except Exception as err:
            logger.debug(f"{QbTrafficStats.LOG_TAG}Cron 表达式「{expression}」解析失败：{err}")
            return None

    @staticmethod
    def __api_url() -> str:
        """
        拼一个可读的调用地址，方便用户直接写进 cron
        """
        try:
            domain = settings.MP_DOMAIN()
        except Exception:
            domain = None
        base = domain or f"http://<MoviePilot地址>:{settings.PORT}"
        return f"{base}{settings.API_V1_STR}/plugin/QbTrafficStats{QbTrafficStats.API_PATH}"

    @staticmethod
    def __source_label(source: Optional[str]) -> str:
        if source == QbTrafficStats.SOURCE_ALLTIME:
            return "全时"
        if source == QbTrafficStats.SOURCE_SESSION:
            return "会话"
        return "-"

    @staticmethod
    def __fmt_bytes(value: Any) -> str:
        """
        字节数转可读文本
        """
        try:
            size = float(value or 0)
        except (TypeError, ValueError):
            size = 0.0
        if size < 0:
            size = 0.0

        units = ["B", "KB", "MB", "GB", "TB", "PB"]
        index = 0
        while size >= 1024 and index < len(units) - 1:
            size /= 1024
            index += 1

        if index == 0:
            return f"{int(size)} B"
        return f"{size:.2f} {units[index]}"

    def __fmt_pair(self, upload: Any, download: Any) -> str:
        """
        详情页里成对展示「上传 / 下载」
        """
        return f"{self.__fmt_bytes(upload)} / {self.__fmt_bytes(download)}"

    @staticmethod
    def __fmt_interval(last_time: Any, now: datetime) -> str:
        """
        距上次采集过了多久
        """
        if not last_time:
            return ""
        try:
            previous = datetime.strptime(str(last_time), QbTrafficStats.TIME_FMT)
        except (TypeError, ValueError):
            return ""
        if previous.tzinfo is None and now.tzinfo is not None:
            previous = previous.replace(tzinfo=now.tzinfo)

        seconds = int((now - previous).total_seconds())
        if seconds <= 0:
            return ""
        if seconds < 60:
            return f"{seconds} 秒"
        if seconds < 3600:
            return f"{seconds // 60} 分钟"
        if seconds < 86400:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours} 小时" + (f" {minutes} 分钟" if minutes else "")
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        return f"{days} 天" + (f" {hours} 小时" if hours else "")

    @staticmethod
    def __to_int(value: Any, default: int = 0) -> int:
        try:
            if value is None or value == "":
                return default
            return int(float(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def __to_list(value: Any) -> List[str]:
        if not value:
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        if isinstance(value, (list, tuple, set)):
            return [str(item).strip() for item in value if str(item).strip()]
        return []
