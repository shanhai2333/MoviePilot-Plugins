"""
QB 流量统计（MoviePilot v2 插件）。

把 qBittorrent 的上传 / 下载流量按「当前累计、较上次新增、今日新增、本月累计、本次会话」
推送到 MoviePilot 的通知渠道。在插件配置里填一个 cron 表达式即可，
由 MoviePilot 自己的调度器定时执行。

关于计数口径与「不能为负」：

- 优先用 qB 的**全时计数**（sync/maindata → server_state.alltime_dl / alltime_ul）。
  它由 qB 自己写在 qBittorrent-data.ini 的 Stats/AllStats 里，重启后仍然保留
  （qB 默认每 15 分钟落盘一次，正常退出也会落盘）。
- 拿不到全时计数时退化为**会话计数**（transfer/info → dl_info_data / up_info_data），
  它每次 qB 重启都会清零，是 qB 源码里注释的 "Data downloaded this session"。
  会话计数另外单独读一份用于展示（就是 qB 状态栏括号里那对数字），不参与增量计算。
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
        "定时推送 qBittorrent 的上传/下载流量：当前累计、较上次新增、今日新增、本月累计、本次会话。"
        "填一个 cron 表达式即可，由 MoviePilot 自己的调度器执行；"
        "详情页附最近 7 天的每日流量与本次会话两张折线图。"
        "统计基于 qB 全时计数，qB 重启或计数回退时自动兜底，不会出现负数。"
    )
    # 插件图标
    plugin_icon = "Qbittorrent_A.png"
    # 插件版本
    plugin_version = "1.8"
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
    # 保留多少天的每日流量（详情页折线图用）
    HISTORY_DAYS = 7
    # 日期格式
    TIME_FMT = "%Y-%m-%d %H:%M:%S"

    # region 私有属性
    # 是否启用
    _enabled: bool = False
    # 选中的下载器名称
    _downloaders: List[str] = []
    # 定时推送的 cron 表达式（留空则不定时，只响应远程命令）
    _cron: str = ""
    # 是否立即运行一次（保存配置后立刻推一次，然后自动复位）
    _onlyonce: bool = False
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
        self._onlyonce = False

        if config:
            self._enabled = bool(config.get("enabled"))
            self._downloaders = self.__to_list(config.get("downloaders"))
            self._cron = str(config.get("cron") or "").strip()
            self._onlyonce = bool(config.get("onlyonce"))

        # 恢复跨会话的统计基准，避免插件重启后把「上次新增」算错
        saved = self.get_data("state")
        if isinstance(saved, dict):
            self._state = saved

        logger.info(
            f"{self.LOG_TAG}配置加载：enabled={self._enabled}, "
            f"下载器={self._downloaders or '未选择'}, "
            f"定时={'未设置' if not self._cron else self._cron}, "
            f"已记录 {len(self._state)} 个下载器的统计基准"
        )

        # 定时表达式校验：写错了直接告诉用户，别等到调度器报错
        if self._cron and self._enabled and not self.__parse_cron(self._cron):
            logger.error(
                f"{self.LOG_TAG}定时表达式「{self._cron}」不合法，定时推送不会生效。"
                f"示例：每小时 0 * * * *，每天 8 点 0 8 * * *，每 6 小时 0 */6 * * *"
            )

        # 立即运行一次：保存配置时触发，跑完把开关拨回去
        if self._onlyonce:
            self._onlyonce = False
            self.update_config(self.__current_config())
            logger.info(f"{self.LOG_TAG}触发立即运行一次")
            self.push_stats(manual=True)

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
        本插件不对外提供 HTTP 接口

        「立即运行一次」走表单开关（onlyonce），保存配置时触发，不需要接口。
        """
        return []

    def __current_config(self) -> Dict[str, Any]:
        """
        当前配置（用于回写表单，「立即运行一次」跑完要把开关复位）
        """
        return {
            "enabled": self._enabled,
            "onlyonce": False,
            "downloaders": self._downloaders,
            "cron": self._cron,
        }

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

        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 2},
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
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即运行一次",
                                            "color": "primary",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "定时推送周期",
                                            "placeholder": "0 * * * *",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
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
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": (
                                                "定时推送由 MoviePilot 自己的调度器执行，填好 cron 表达式保存即可。"
                                                "5 段式（分 时 日 月 周），例如每小时 0 * * * *、"
                                                "每天 8 点 0 8 * * *、每 6 小时 0 */6 * * *；留空则不定时。"
                                                "「较上次新增」是距上一次推送之间的增量，"
                                                "周期越长这个数字覆盖的时间跨度越大。"
                                                "「立即运行一次」打开后保存即推一次，开关会自动复位。"
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
                                                "「本次会话」是单独读的会话计数，就是 qB 状态栏括号里那对数字，"
                                                "只用于展示，不参与增量计算。"
                                                "两种口径都做了回退检测，计数器变小就按 0 计或按当前值计，"
                                                "不会出现负数。「今日新增」「本月累计」由本插件按增量累加，"
                                                "跨天、跨月自动重新起算；跨零点的那一轮增量会整段计入新的一天，"
                                                "所以建议配合每小时级别的 cron 使用，间隔越长这一段的归属越粗。"
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
            "onlyonce": False,
            "cron": "0 * * * *",
            "downloaders": [],
        }

    def get_page(self) -> List[dict]:
        """
        插件详情页：近 N 天流量折线图 + 各下载器明细
        """
        state = self._state or {}
        if not state:
            return [
                {
                    "component": "VAlert",
                    "props": {
                        "type": "info",
                        "variant": "tonal",
                        "text": "暂无统计数据。插件启用并到点推送一次后，这里会显示各下载器的流量。",
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
                        {"component": "td", "text": self.__fmt_sess(item)},
                        {"component": "td", "text": item.get("last_time") or "-"},
                    ],
                }
            )

        contents = []
        daily_chart = self.__build_chart(
            state, "history", "近 {days} 天流量（{unit}）")
        if daily_chart:
            contents.append(daily_chart)
        sess_chart = self.__build_chart(
            state, "sess_history", "本次会话累计 · 近 {days} 天（{unit}）")
        if sess_chart:
            contents.append(sess_chart)
        contents.append(
            {
                "component": "VCol",
                "props": {"cols": 12},
                "content": [
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
                                            {"component": "th", "text": "本次会话（上/下）"},
                                            {"component": "th", "text": "上次采集"},
                                        ],
                                    }
                                ],
                            },
                            {"component": "tbody", "content": rows},
                        ],
                    }
                ],
            }
        )

        return [{"component": "VRow", "content": contents}]

    def __build_chart(self, state: Dict[str, Dict[str, Any]], history_key: str,
                      title_fmt: str) -> Optional[dict]:
        """
        近 HISTORY_DAYS 天的折线图（多下载器按日期汇总）

        history_key 为 "history" 时画的是每日流量，
        为 "sess_history" 时画的是会话计数当天的最终值（qB 重启会掉回 0）。
        """
        daily: Dict[str, Dict[str, int]] = {}
        for item in state.values():
            history = item.get(history_key)
            if not isinstance(history, list):
                continue
            for entry in history:
                if not isinstance(entry, dict):
                    continue
                date = str(entry.get("date") or "").strip()
                if not date:
                    continue
                bucket = daily.setdefault(date, {"dl": 0, "ul": 0})
                bucket["dl"] += max(self.__to_int(entry.get("dl")), 0)
                bucket["ul"] += max(self.__to_int(entry.get("ul")), 0)

        if not daily:
            return None

        dates = sorted(daily)[-self.HISTORY_DAYS:]
        peak = max(max(daily[d]["dl"], daily[d]["ul"]) for d in dates)
        divisor, unit = self.__pick_unit(peak)

        categories = [date[5:] for date in dates]
        # 单位写进曲线名，这样 tooltip 和 legend 里都带单位（ApexCharts 的
        # formatter 得传 JS 函数，走配置 JSON 传不了）
        series = [
            {"name": f"上传（{unit}）", "data": [round(daily[d]["ul"] / divisor, 2) for d in dates]},
            {"name": f"下载（{unit}）", "data": [round(daily[d]["dl"] / divisor, 2) for d in dates]},
        ]

        title = title_fmt.format(days=len(dates), unit=unit)
        if dates[-1] == datetime.now(pytz.timezone(settings.TZ)).strftime("%Y-%m-%d"):
            title += " · 今天仍在累计"

        return {
            "component": "VCol",
            "props": {"cols": 12},
            "content": [
                {
                    "component": "VApexChart",
                    "props": {
                        "height": 300,
                        "options": {
                            "chart": {"type": "line", "zoom": {"enabled": False}},
                            "title": {"text": title},
                            "xaxis": {"categories": categories},
                            "stroke": {"curve": "smooth", "width": 2},
                            "markers": {"size": 4},
                            "legend": {"show": True},
                            "tooltip": {"shared": True},
                            "dataLabels": {"enabled": False},
                            "noData": {"text": "暂无数据"},
                            "yaxis": {"title": {"text": unit}},
                        },
                        "series": series,
                    },
                }
            ],
        }

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
        # 会话计数单独读一份：只用于展示和折线图，不参与增量计算
        session = (cur_dl, cur_ul) if source == self.SOURCE_SESSION \
            else self.__read_session(qb)

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

        # 3) 会话计数：读不到就沿用上次的值，别把折线图打成 0
        sess_dl = state.get("sess_dl")
        sess_ul = state.get("sess_ul")
        sess_time = state.get("sess_time")
        sess_history = state.get("sess_history")
        if session:
            sess_dl, sess_ul = session
            sess_time = now.strftime(self.TIME_FMT)
            sess_history = self.__merge_history(sess_history, today, sess_dl, sess_ul)

        # 4) 保存状态
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
            # 会话计数（qB 状态栏括号里那对数字，重启清零）
            "sess_dl": sess_dl,
            "sess_ul": sess_ul,
            "sess_time": sess_time,
            "sess_history": sess_history,
            # 最近 HISTORY_DAYS 天的每日流量，供详情页画折线图
            "history": self.__merge_history(
                state.get("history"), today, today_dl, today_ul),
        }

        interval = "" if first_run else self.__fmt_interval(state.get("last_time"), now)
        logger.info(
            f"{self.LOG_TAG}[{name}] 数据源={self.__source_label(source)}, "
            f"累计 上传{self.__fmt_bytes(cur_ul)}/下载{self.__fmt_bytes(cur_dl)}, "
            f"本次新增 上传{self.__fmt_bytes(delta_ul)}/下载{self.__fmt_bytes(delta_dl)}, "
            f"今日 上传{self.__fmt_bytes(today_ul)}/下载{self.__fmt_bytes(today_dl)}, "
            f"本月 上传{self.__fmt_bytes(month_ul)}/下载{self.__fmt_bytes(month_dl)}, "
            f"本次会话 上传{self.__fmt_bytes(sess_ul)}/下载{self.__fmt_bytes(sess_dl)}"
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
            "sess_dl": sess_dl,
            "sess_ul": sess_ul,
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
            if item.get("sess_dl") is not None or item.get("sess_ul") is not None:
                lines.append(
                    f"本次会话：上传 {self.__fmt_bytes(item.get('sess_ul'))}"
                    f" / 下载 {self.__fmt_bytes(item.get('sess_dl'))}"
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

    def __fmt_sess(self, item: Dict[str, Any]) -> str:
        """
        本次会话（qB 状态栏括号里那对数字），读不到就显示 -
        """
        if item.get("sess_dl") is None and item.get("sess_ul") is None:
            return "-"
        return self.__fmt_pair(item.get("sess_ul"), item.get("sess_dl"))

    @staticmethod
    def __pick_unit(peak: int) -> Tuple[int, str]:
        """
        按峰值挑一个合适的单位，避免满屏 0.00
        """
        for threshold, unit in ((1024 ** 5, "PB"), (1024 ** 4, "TB"),
                                (1024 ** 3, "GB"), (1024 ** 2, "MB")):
            if peak >= threshold:
                return threshold, unit
        return 1024, "KB"

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

    @classmethod
    def __merge_history(cls, history: Any, today: str,
                        today_dl: int, today_ul: int) -> List[Dict[str, Any]]:
        """
        维护最近 HISTORY_DAYS 天的每日流量

        旧日期原样保留，今天用当天的累计值覆盖，最后按日期排序截断。
        用当天的累计值而不是增量，避免同一天多次采集时重复累加。
        """
        items: Dict[str, Dict[str, Any]] = {}
        if isinstance(history, list):
            for item in history:
                if not isinstance(item, dict):
                    continue
                date = str(item.get("date") or "").strip()
                if not date or date == today:
                    continue
                items[date] = {
                    "date": date,
                    "dl": max(cls.__to_int(item.get("dl")), 0),
                    "ul": max(cls.__to_int(item.get("ul")), 0),
                }
        items[today] = {
            "date": today,
            "dl": max(cls.__to_int(today_dl), 0),
            "ul": max(cls.__to_int(today_ul), 0),
        }
        return sorted(items.values(), key=lambda entry: entry["date"])[-cls.HISTORY_DAYS:]

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
