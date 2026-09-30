from typing import Any, Dict, List, Tuple

from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase


class MediaExtPlus(_PluginBase):
    # 插件名称
    plugin_name = "自定义媒体扩展名"
    # 插件描述
    plugin_desc = "把 cas 等自定义扩展名追加进 MoviePilot 的媒体文件扩展名白名单，让整理、识别、刮削能处理这些格式（如天翼云盘秒传文件 .cas）。"
    # 插件图标
    plugin_icon = "189drive_A.png"
    # 插件版本
    plugin_version = "1.0"
    # 插件作者
    plugin_author = "shanhai2333"
    # 作者主页
    author_url = "https://github.com/shanhai2333"
    # 插件配置项ID前缀
    plugin_config_prefix = "mediaextplus_"
    # 加载顺序
    plugin_order = 30
    # 可使用的用户级别
    auth_level = 1

    # 日志前缀
    LOG_TAG = "[自定义媒体扩展名]"

    # 私有属性
    _enabled: bool = False
    _exts: str = "cas"

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._exts = config.get("exts") if config.get("exts") is not None else "cas"

        # 先撤掉上一次注入的，再按当前配置重新注入，避免改了配置后残留
        # （用 save_data 持久化，插件实例被重建后也还原得回来）
        for ext in self.get_data("injected") or []:
            if ext in settings.RMT_MEDIAEXT:
                settings.RMT_MEDIAEXT.remove(ext)

        if not self._enabled:
            self.save_data("injected", [])
            logger.info(f"{self.LOG_TAG}未启用，不修改媒体文件扩展名")
            return

        exts = self.__parse_exts(self._exts)
        if not exts:
            self.save_data("injected", [])
            logger.warn(f"{self.LOG_TAG}未配置有效扩展名，不修改媒体文件扩展名")
            return

        # settings 是模块级单例，整理链运行时读的就是这个对象，append 立即生效
        added = []
        for ext in exts:
            if ext not in settings.RMT_MEDIAEXT:
                settings.RMT_MEDIAEXT.append(ext)
                added.append(ext)
        self.save_data("injected", added)

        logger.info(
            f"{self.LOG_TAG}媒体文件扩展名已生效，本次新增：{','.join(added) if added else '无'}，"
            f"当前共 {len(settings.RMT_MEDIAEXT)} 个"
        )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """
        本插件不对外提供 HTTP 接口
        """
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
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
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "exts",
                                            "label": "追加的扩展名",
                                            "placeholder": "cas",
                                            "hint": "多个用逗号分隔，带不带点都行",
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
                                            "text": "MoviePilot 判断「哪些文件算媒体文件」只看扩展名白名单"
                                                    "（settings.RMT_MEDIAEXT，默认 18 个，含 .strm、不含 .cas），"
                                                    "全程不读文件内容 —— .strm 能被整理，只是因为它在这个白名单里。"
                                                    "本插件把这里配置的扩展名追加进该白名单，保存即生效。",
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
                                            "text": "注意：这只解决「MP 能识别并整理」，不解决「媒体服务器能播放」。"
                                                    ".strm 能播是因为文件里写的是播放链接，而 .cas 里是秒传指纹"
                                                    "（Base64 编码的 JSON：name/size/md5/sliceMd5），"
                                                    "Emby/Jellyfin 读不出可播地址。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "exts": "cas",
        }

    def get_page(self) -> List[dict]:
        """
        详情页：显示当前 MP 实际生效的媒体文件扩展名
        """
        current = list(settings.RMT_MEDIAEXT)
        injected = self.get_data("injected") or []
        return [
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
                                    "type": "success" if injected else "info",
                                    "variant": "tonal",
                                    "text": f"本插件注入：{'、'.join(injected) if injected else '无'}",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "text": f"MP 当前生效的媒体文件扩展名共 {len(current)} 个："
                                            f"{'、'.join(current)}",
                                },
                            }
                        ],
                    },
                ],
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """
        纯配置型插件，不需要定时任务
        """
        return []

    def stop_service(self):
        """
        停用时把注入的扩展名撤掉，恢复 MP 原样
        """
        for ext in self.get_data("injected") or []:
            if ext in settings.RMT_MEDIAEXT:
                settings.RMT_MEDIAEXT.remove(ext)
        self.save_data("injected", [])
        logger.info(f"{self.LOG_TAG}插件已停用，媒体文件扩展名已还原")

    @staticmethod
    def __parse_exts(raw) -> List[str]:
        """
        解析扩展名配置：支持中英文逗号/顿号分隔、可不带点、去重
        """
        exts: List[str] = []
        text = str(raw or "").replace("，", ",").replace("、", ",")
        for part in text.split(","):
            ext = part.strip().lower()
            if not ext:
                continue
            if not ext.startswith("."):
                ext = f".{ext}"
            if ext not in exts:
                exts.append(ext)
        return exts
