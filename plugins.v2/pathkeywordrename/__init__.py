import os
import threading
from typing import Any, Dict, List, Tuple, Optional

from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.event import TransferRenameEventData
from app.schemas.types import ChainEventType

lock = threading.Lock()


class PathKeywordRename(_PluginBase):
    # 插件名称
    plugin_name = "路径关键字重命名"
    # 插件描述
    plugin_desc = "根据文件目标路径中的关键字，将对应的目录名附加到文件名末尾。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/InfinityPacer/MoviePilot-Plugins/main/icons/smartrename.png"
    # 插件版本
    plugin_version = "1.4"
    # 插件作者
    plugin_author = "shanhai2333"
    # 作者主页
    author_url = "https://github.com/shanhai2333"
    # 插件配置项ID前缀
    plugin_config_prefix = "pathrename_"
    # 加载顺序
    plugin_order = 43
    # 可使用的用户级别
    auth_level = 1

    # region 私有属性
    # 是否开启
    _enabled = False
    # 路径关键字
    _path_keyword: Optional[str] = None
    # 路径关键字分隔符
    _path_keyword_separator: Optional[str] = " "

    # endregion

    def init_plugin(self, config: dict = None):
        if not config:
            return

        self._enabled = config.get("enabled") or False
        self._path_keyword = config.get("path_keyword")
        self._path_keyword_separator = config.get("path_keyword_separator") or " "

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                            'hint': '开启后插件将处于激活状态',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'path_keyword',
                                            'label': '路径关键字',
                                            'hint': '当目标路径的目录名包含此关键字时，将该目录名附加到文件名末尾',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'path_keyword_separator',
                                            'label': '路径关键字分隔符',
                                            'hint': '文件名与附加的目录名之间的分隔符，默认为空格',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "path_keyword": "",
            "path_keyword_separator": " "
        }

    def get_page(self) -> List[dict]:
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        pass

    def stop_service(self):
        pass

    @eventmanager.register(ChainEventType.TransferRename)
    def handle_transfer_rename(self, event: Event):
        """
        处理 TransferRename 事件
        :param event: 事件数据
        """
        if not self._enabled or not event or not event.event_data:
            return

        event_data: TransferRenameEventData = event.event_data

        if event_data.updated:
            logger.debug(f"该事件已被其他事件处理器处理，跳过后续操作")
            return

        try:
            updated_str = event_data.render_str

            # 路径关键字处理
            if self._path_keyword and hasattr(event_data, 'path') and event_data.path:
                logger.debug(f"路径关键字功能已启用，关键字: '{self._path_keyword}', 目标路径: '{event_data.path}'")
                # 获取目录路径
                dir_path = os.path.dirname(event_data.path)
                # 分割路径
                path_parts = dir_path.replace("\\", "/").split("/")
                for part in reversed(path_parts):
                    if self._path_keyword in part:
                        logger.info(f"在路径中找到关键字 '{self._path_keyword}' 于目录 '{part}'，将把目录名附加到文件名中。")
                        separator = self._path_keyword_separator or ' '
                        updated_str = f"{updated_str}{separator}{part}"
                        logger.debug(f"附加目录名后的字符串: {updated_str}")
                        break

            # 仅在有实际更新时，标记更新状态
            if updated_str and updated_str != event_data.render_str:
                event_data.updated_str = updated_str
                event_data.updated = True
                event_data.source = self.plugin_name
                logger.info(f"重命名完成，{event_data.render_str} -> {updated_str}")
            else:
                logger.debug(f"重命名结果与原始值相同，跳过更新")
        except Exception as e:
            logger.error(f"重命名发生未知异常: {e}", exc_info=True)