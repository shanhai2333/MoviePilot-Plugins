import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from app.core.config import settings
from app.core.event import Event, eventmanager
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType, ServiceInfo
from app.schemas.types import EventType


class QbSpeedSwitch(_PluginBase):
    # 插件名称
    plugin_name = "QB速度调度"
    # 插件描述
    plugin_desc = (
        "按下载状态自动切换 qBittorrent 上传/下载限速：无下载时固定限速（防 PCDN 判定），"
        "有下载时解除限速让下载跑满；下载中实测上传速度超过阈值时压制上传限速（只压不自动恢复，"
        "下载完成后自动恢复无下载时的固定限速）。"
    )
    # 插件图标
    plugin_icon = "Qbittorrent_A.png"
    # 插件版本
    plugin_version = "1.0"
    # 插件作者
    plugin_author = "shanhai2333"
    # 作者主页
    author_url = "https://github.com/shanhai2333"
    # 插件配置项ID前缀
    plugin_config_prefix = "qbspeedswitch_"
    # 加载顺序
    plugin_order = 22
    # 可使用的用户级别
    auth_level = 1

    # 日志前缀
    LOG_TAG = "[QB速度调度]"
    # 场景标识
    SCENE_DOWNLOADING = "downloading"
    SCENE_IDLE = "idle"

    # region 私有属性
    # 是否启用
    _enabled: bool = False
    # 是否发送通知
    _notify: bool = False
    # 是否立即运行一次
    _onlyonce: bool = False
    # 选中的下载器名称
    _downloaders: List[str] = []
    # 检测间隔（秒）
    _interval: int = 30
    # 有下载时（downloading）
    _downloading_upload_limit: int = 0
    _downloading_download_limit: int = 0
    # 无下载时（idle）
    _idle_upload_limit: int = 200
    _idle_download_limit: int = 0
    # 上传超速压制
    _threshold_enabled: bool = False
    _upload_threshold: int = 0
    _threshold_upload_limit: int = 500
    # 各下载器运行状态 {下载器名: {scene, throttled, dl, ul, ...}}
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
            self._notify = bool(config.get("notify"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._downloaders = self.__to_list(config.get("downloaders"))
            self._interval = max(self.__to_int(config.get("interval"), 30), 10)

            self._downloading_upload_limit = max(
                self.__to_int(config.get("downloading_upload_limit"), 0), 0)
            self._downloading_download_limit = max(
                self.__to_int(config.get("downloading_download_limit"), 0), 0)
            self._idle_upload_limit = max(self.__to_int(config.get("idle_upload_limit"), 200), 0)
            self._idle_download_limit = max(self.__to_int(config.get("idle_download_limit"), 0), 0)

            self._threshold_enabled = bool(config.get("threshold_enabled"))
            self._upload_threshold = max(self.__to_int(config.get("upload_threshold"), 0), 0)
            self._threshold_upload_limit = max(
                self.__to_int(config.get("threshold_upload_limit"), 500), 0)

        # 恢复跨会话状态，避免插件重启后重复压制
        saved = self.get_data("state")
        if isinstance(saved, dict):
            self._state = saved

        logger.info(
            f"{self.LOG_TAG}配置加载：enabled={self._enabled}, notify={self._notify}, "
            f"下载器={self._downloaders or '未选择'}, 间隔={self._interval}s, "
            f"无下载时[下载{self._idle_download_limit}/上传{self._idle_upload_limit}]KB/s, "
            f"有下载时[下载{self._downloading_download_limit}/上传{self._downloading_upload_limit}]KB/s, "
            f"超速压制={self._threshold_enabled}"
            f"(阈值{self._upload_threshold}→{self._threshold_upload_limit})KB/s"
        )

        # 动态限流参数校验
        if self._threshold_enabled and (self._upload_threshold <= 0 or self._threshold_upload_limit <= 0):
            logger.warning(
                f"{self.LOG_TAG}动态限流已启用，但「上传速度阈值」或「压制后上传限速」为 0，"
                f"动态限流不会生效，请补齐配置。"
            )

        # 立即运行一次
        if self._onlyonce:
            self._onlyonce = False
            self.update_config(self.__current_config())
            logger.info(f"{self.LOG_TAG}触发立即运行一次")
            self.sync_speed(manual=True)
    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        定义远程控制命令
        """
        return [
            {
                "cmd": "/qb_speed_sync",
                "event": EventType.PluginAction,
                "desc": "立即执行一次QB速度调度",
                "category": "QB",
                "data": {"action": "qb_speed_sync"},
            },
            {
                "cmd": "/qb_speed_status",
                "event": EventType.PluginAction,
                "desc": "查看QB速度调度当前状态",
                "category": "QB",
                "data": {"action": "qb_speed_status"},
            },
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        """
        if not self._enabled:
            return []

        return [
            {
                "id": "QbSpeedSwitch",
                "name": "QB速度调度服务",
                "trigger": "interval",
                "func": self.sync_speed,
                "kwargs": {"seconds": self._interval},
            }
        ]

    def stop_service(self):
        """
        退出插件
        """
        logger.info(f"{self.LOG_TAG}插件已停止（已下发的限速值会保留，不会自动恢复）")

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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                            "hint": "开启后按设定间隔自动同步限速",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "发送通知",
                                            "hint": "限速值实际变化时推送消息",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即运行一次",
                                            "hint": "保存后立刻执行一次检测",
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
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                            "model": "downloaders",
                                            "label": "下载器",
                                            "hint": "仅支持 qBittorrent，可多选",
                                            "persistent-hint": True,
                                            "items": downloader_items,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "interval",
                                            "label": "检测间隔（秒）",
                                            "hint": "最小 10 秒，建议 30 秒",
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
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": (
                                                "① 无下载时（平时）—— 固定限速。"
                                                "没有任何下载任务时用这组值，把上传压住，避免上传过多被运营商判定为 PCDN。"
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
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "idle_upload_limit",
                                            "label": "上传限速（KB/s）",
                                            "hint": "平时的固定上传速度，例如 200；0 表示不限速",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "idle_download_limit",
                                            "label": "下载限速（KB/s）",
                                            "hint": "无下载任务时基本用不到，一般留 0",
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
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": (
                                                "② 有下载时 —— 解除限速让下载跑满。"
                                                "只要存在下载中任务（含卡种）就用这组值，上传限速填 0 即不限速；"
                                                "上传若飙起来，由下面的「上传超速压制」接管。"
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
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "downloading_upload_limit",
                                            "label": "上传限速（KB/s）",
                                            "hint": "填 0 = 不限速，让下载尽快完成",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "downloading_download_limit",
                                            "label": "下载限速（KB/s）",
                                            "hint": "一般留 0（不限速）",
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
                                        "component": "VAlert",
                                        "props": {
                                            "type": "warning",
                                            "variant": "tonal",
                                            "text": (
                                                "③ 上传超速压制 —— 实测上传速度超过阈值时，把上传限速压到指定值。"
                                                "只压不自动恢复：触发后一直保持，直到下载全部完成（切回无下载时的固定限速）才重置；"
                                                "下次再来下载时重新从「不限速」开始，超了再压。"
                                                "无下载时上传已被固定限速，通常不会触发这一条。"
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
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "threshold_enabled",
                                            "label": "启用超速压制",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "upload_threshold",
                                            "label": "实测上传超过（KB/s）",
                                            "hint": "触发阈值，必须大于 0",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "threshold_upload_limit",
                                            "label": "压制到（KB/s）",
                                            "hint": "触发后压到的上传速度，例如 500",
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
                                        "component": "VAlert",
                                        "props": {
                                            "type": "warning",
                                            "variant": "tonal",
                                            "text": (
                                                "关于「档位」：本插件通过 set_speed_limit 下发限速，写入的是 qBittorrent "
                                                "当前生效的档位 —— 备用速度模式关闭时写全局限速，开启时写备用限速。"
                                                "如果 qB 开了计划限速（自动切换备用速度），切换瞬间生效值会变成另一个槽位的旧值，"
                                                "插件会在下一轮检测时自动纠正，最长延迟为一个检测间隔。"
                                                "为避免 qB 自带计划限速与本插件互相干扰，建议关闭 qB 的计划限速，由本插件统一接管。"
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
            "notify": False,
            "onlyonce": False,
            "downloaders": [],
            "interval": 30,
            "idle_upload_limit": 200,
            "idle_download_limit": 0,
            "downloading_upload_limit": 0,
            "downloading_download_limit": 0,
            "threshold_enabled": False,
            "upload_threshold": 0,
            "threshold_upload_limit": 500,
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
                        "text": "暂无运行数据。插件启用并完成首轮检测后，这里会显示各下载器的当前档位。",
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
                        {"component": "td", "text": self.__scene_label(item.get("scene"))},
                        {"component": "td", "text": "是" if item.get("throttled") else "否"},
                        {"component": "td", "text": self.__fmt_limit(item.get("dl"))},
                        {"component": "td", "text": self.__fmt_limit(item.get("ul"))},
                        {"component": "td", "text": str(item.get("downloading", 0))},
                        {"component": "td", "text": item.get("updated_at") or "-"},
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
                                    {"component": "th", "text": "当前场景"},
                                    {"component": "th", "text": "已压制"},
                                    {"component": "th", "text": "下载限速"},
                                    {"component": "th", "text": "上传限速"},
                                    {"component": "th", "text": "下载中任务"},
                                    {"component": "th", "text": "更新时间"},
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
        if action == "qb_speed_sync":
            self.sync_speed(manual=True)
        elif action == "qb_speed_status":
            self.__report_status()

    def sync_speed(self, manual: bool = False):
        """
        执行一次速度同步
        """
        if not self._enabled and not manual:
            return

        services = self.__get_services()
        if not services:
            return

        with self._lock:
            for name, service in services.items():
                try:
                    self.__sync_one(name, service)
                except Exception as err:
                    logger.error(f"{self.LOG_TAG}处理下载器 [{name}] 出错：{err}", exc_info=True)
            self.save_data("state", self._state)

    def __sync_one(self, name: str, service: ServiceInfo):
        """
        同步单个下载器
        """
        qb = getattr(service, "instance", None)
        if not qb:
            logger.warning(f"{self.LOG_TAG}下载器 [{name}] 实例不存在，跳过")
            return

        # 1) 场景判定：有 downloading 状态的任务（含卡种）即视为下载中
        torrents = qb.get_downloading_torrents()
        if torrents is None:
            logger.warning(f"{self.LOG_TAG}下载器 [{name}] 读取下载中任务失败，跳过本轮")
            return
        scene = self.SCENE_DOWNLOADING if torrents else self.SCENE_IDLE

        # 2) 场景切换时重置压制标记
        state = self._state.get(name) or {}
        previous_scene = state.get("scene")
        scene_changed = previous_scene != scene
        if scene_changed:
            state = {"scene": scene, "throttled": False}

        # 3) 取当前场景的目标档位
        if scene == self.SCENE_DOWNLOADING:
            target_dl, target_ul = self._downloading_download_limit, self._downloading_upload_limit
        else:
            target_dl, target_ul = self._idle_download_limit, self._idle_upload_limit

        # 4) 动态限流：只压不自动恢复
        # 4) 动态限流：只压不自动恢复
        # 档位基准值（未压制时该场景应有的上传限速）
        scene_ul = target_ul
        # 压制值必须比档位值更严格，否则「压制」反而等于放宽限速。
        # 这个判断每轮都要做：用户可能在压制生效期间改配置，导致旧压制值不再合理。
        throttle_usable = self._threshold_upload_limit > 0 and (
            scene_ul == 0 or self._threshold_upload_limit < scene_ul
        )

        # 关闭开关、或压制值不再合理时，同步解除压制状态
        was_throttled = bool(state.get("throttled"))
        throttled = was_throttled and self._threshold_enabled and throttle_usable
        if was_throttled and not throttled:
            reason = "动态限流已关闭" if not self._threshold_enabled else "压制值不再比档位值严格"
            logger.info(f"{self.LOG_TAG}[{name}] 压制已解除（{reason}）")

        # 未处于压制时，评估是否需要进入压制
        if not throttled and self._threshold_enabled and self._upload_threshold > 0:
            up_speed = self.__upload_speed(qb)
            if up_speed is not None and up_speed > self._upload_threshold:
                if throttle_usable:
                    throttled = True
                    logger.info(
                        f"{self.LOG_TAG}[{name}] 实测上传 {up_speed:.0f} KB/s 超过阈值 "
                        f"{self._upload_threshold} KB/s，压制上传限速至 {self._threshold_upload_limit} KB/s"
                    )
                elif not state.get("warned"):
                    logger.warning(
                        f"{self.LOG_TAG}[{name}] 实测上传 {up_speed:.0f} KB/s 超过阈值，"
                        f"但压制值 {self._threshold_upload_limit} KB/s 不低于档位值 {scene_ul} KB/s，忽略本次压制"
                    )
                    state["warned"] = True

        if throttled:
            target_ul = self._threshold_upload_limit

        # 5) 与实际值比对，有差异才下发
        current_dl, current_ul = self.__current_limits(qb)
        need_apply = current_dl is None or current_dl != target_dl or current_ul != target_ul

        if need_apply:
            if qb.set_speed_limit(download_limit=target_dl, upload_limit=target_ul):
                logger.info(
                    f"{self.LOG_TAG}[{name}] 场景={self.__scene_label(scene)}"
                    f"{'（已压制）' if throttled else ''}，"
                    f"下发限速：下载 {self.__fmt_limit(target_dl)} / 上传 {self.__fmt_limit(target_ul)}"
                )
                if self._notify:
                    self.__notify_change(
                        name=name,
                        scene=scene,
                        scene_changed=scene_changed,
                        just_throttled=throttled and not was_throttled,
                        throttled=throttled,
                        download_limit=target_dl,
                        upload_limit=target_ul,
                    )
            else:
                logger.error(f"{self.LOG_TAG}[{name}] 下发限速失败")
                if self._notify:
                    self.post_message(
                        mtype=NotificationType.Plugin,
                        title="【QB速度调度】",
                        text=f"下载器 [{name}] 下发限速失败，请检查 qBittorrent 连接。",
                    )
        else:
            logger.debug(f"{self.LOG_TAG}[{name}] 限速值无需变更")

        # 6) 记录状态
        state.update(
            {
                "scene": scene,
                "throttled": throttled,
                "dl": target_dl,
                "ul": target_ul,
                "downloading": len(torrents),
                "updated_at": datetime.now(pytz.timezone(settings.TZ)).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        self._state[name] = state

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

    @staticmethod
    def __upload_speed(qb) -> Optional[float]:
        """
        读取 qBittorrent 全局实测上传速度，单位 KB/s
        """
        try:
            info = qb.transfer_info()
        except Exception as err:
            logger.error(f"{QbSpeedSwitch.LOG_TAG}读取传输信息出错：{err}")
            return None

        if not info or not hasattr(info, "get"):
            return None

        for key in ("up_info_speed", "up_speed", "upSpeed"):
            value = info.get(key)
            if value is None:
                continue
            try:
                return float(value) / 1024
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def __current_limits(qb) -> Tuple[Optional[int], Optional[int]]:
        """
        读取 qBittorrent 当前生效档位的 (下载限速, 上传限速)，单位 KB/s

        注意：qBittorrent 内部用 -1 表示不限速，经 MoviePilot 除以 1024 后会是
        -0.0009765625 这类极小负数，这里统一归零，保证与「0 表示不限速」的语义一致。
        """
        try:
            current = qb.get_speed_limit()
        except Exception as err:
            logger.error(f"{QbSpeedSwitch.LOG_TAG}读取当前限速出错：{err}")
            return None, None

        if not isinstance(current, (tuple, list)) or len(current) != 2:
            return None, None

        try:
            download_limit = max(round(float(current[0] or 0)), 0)
            upload_limit = max(round(float(current[1] or 0)), 0)
            return download_limit, upload_limit
        except (TypeError, ValueError):
            return None, None

    def __notify_change(self, name: str, scene: str, scene_changed: bool, just_throttled: bool,
                        throttled: bool, download_limit: int, upload_limit: int):
        """
        限速值变化通知
        """
        if scene_changed:
            title = f"【QB速度调度】已切到{self.__scene_label(scene)}状态"
        elif just_throttled:
            title = "【QB速度调度】已触发上传压制"
        elif throttled:
            title = "【QB速度调度】压制中，限速被重新下发"
        else:
            title = "【QB速度调度】限速已更新"

        text = (
            f"下载器：{name}\n"
            f"场景：{self.__scene_label(scene)}\n"
            f"下载限速：{self.__fmt_limit(download_limit)}\n"
            f"上传限速：{self.__fmt_limit(upload_limit)}"
        )
        if throttled:
            text += f"\n\n已压制：实测上传超过 {self._upload_threshold} KB/s，保持压制直到场景切换。"

        self.post_message(mtype=NotificationType.Plugin, title=title, text=text)

    def __report_status(self):
        """
        输出当前状态（用于远程命令）
        """
        if not self._state:
            self.post_message(
                mtype=NotificationType.Plugin,
                title="【QB速度调度】",
                text="暂无运行数据，请确认插件已启用且已选择下载器。",
            )
            return

        lines = []
        for name, item in self._state.items():
            lines.append(
                f"{name}：{self.__scene_label(item.get('scene'))}"
                f"{'（已压制）' if item.get('throttled') else ''}，"
                f"下载 {self.__fmt_limit(item.get('dl'))} / 上传 {self.__fmt_limit(item.get('ul'))}，"
                f"下载中 {item.get('downloading', 0)} 个"
            )

        self.post_message(
            mtype=NotificationType.Plugin,
            title="【QB速度调度】当前状态",
            text="\n".join(lines),
        )

    def __current_config(self) -> Dict[str, Any]:
        """
        当前配置（用于回写表单）
        """
        return {
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": False,
            "downloaders": self._downloaders,
            "interval": self._interval,
            "downloading_download_limit": self._downloading_download_limit,
            "downloading_upload_limit": self._downloading_upload_limit,
            "idle_download_limit": self._idle_download_limit,
            "idle_upload_limit": self._idle_upload_limit,
            "threshold_enabled": self._threshold_enabled,
            "upload_threshold": self._upload_threshold,
            "threshold_upload_limit": self._threshold_upload_limit,
        }

    @staticmethod
    def __scene_label(scene: Optional[str]) -> str:
        if scene == QbSpeedSwitch.SCENE_DOWNLOADING:
            return "有下载"
        if scene == QbSpeedSwitch.SCENE_IDLE:
            return "无下载"
        return "未知"

    @staticmethod
    def __fmt_limit(limit: Any) -> str:
        try:
            value = int(limit or 0)
        except (TypeError, ValueError):
            value = 0
        return "不限速" if value <= 0 else f"{value} KB/s"

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
