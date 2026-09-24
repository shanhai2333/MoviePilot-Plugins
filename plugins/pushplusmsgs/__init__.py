from typing import Any, List, Dict, Tuple

from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, NotificationType
from app.utils.http import RequestUtils


class PushPlusMsgs(_PluginBase):
    # 插件名称
    plugin_name = "PushPlus消息推送(群发)"
    # 插件描述
    plugin_desc = "使用PushPlus发送消息通知，支持群发；媒体类通知会带上海报图片。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/shanhai2333/MoviePilot-Plugins/main/icons/Pushplusplugin.png"
    # 插件版本
    plugin_version = "1.2"
    # 插件作者
    plugin_author = "cheng,shanhai2333"
    # 作者主页
    author_url = "https://github.com/shanhai2333"
    # 插件配置项ID前缀
    plugin_config_prefix = "pushplusmsgs_"
    # 加载顺序
    plugin_order = 27
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled = False
    _istopic = False
    _topicid = None
    _token = None
    _msgtypes = []

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._istopic = config.get("istopic")
            self._topicid = config.get("topicid")
            self._token = config.get("token")
            self._msgtypes = config.get("msgtypes") or []

    def get_state(self) -> bool:
        return self._enabled and (True if self._token else False)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        # 编历 NotificationType 枚举，生成消息类型选项
        MsgTypeOptions = []
        for item in NotificationType:
            MsgTypeOptions.append({
                "title": item.value,
                "value": item.name
            })
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
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'istopic',
                                            'label': '启用群发',
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
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'token',
                                            'label': 'PushPlus令牌',
                                            'placeholder': 'c3f0**',
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
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'topicid',
                                            'label': '群组id',
                                            'placeholder': 'c3f0**',
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
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'multiple': True,
                                            'chips': True,
                                            'model': 'msgtypes',
                                            'label': '消息类型',
                                            'items': MsgTypeOptions
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
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '由于pushplus规则更新，没有实名认证的用户无法发送消息，所以需要用户自己去官网进行认证。官网地址:https://www.pushplus.plus'
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
            "istopic": False,
            "topicid": '',
            'token': '',
            'msgtypes': []
        }

    def get_page(self) -> List[dict]:
        pass

    @eventmanager.register(EventType.NoticeMessage)
    def send(self, event: Event):
        """
        消息发送事件
        """
        if not self.get_state():
            return

        if not event.event_data:
            return

        msg_body = event.event_data
        # 渠道
        channel = msg_body.get("channel")
        if channel:
            return
        # 类型
        msg_type: NotificationType = msg_body.get("type")
        # 标题
        title = msg_body.get("title")
        # 文本
        text = msg_body.get("text")
        # 图片（MP 在媒体类通知里会带上，来源是 TMDB 的海报/背景图，形如
        # https://{TMDB_IMAGE_DOMAIN}/t/p/w500/xxx.jpg；非媒体通知为空）
        image = msg_body.get("image")

        if not title and not text:
            logger.warn("标题和内容不能同时为空")
            return

        if (msg_type and self._msgtypes
                and msg_type.name not in self._msgtypes):
            logger.info(f"消息类型 {msg_type.value} 未开启消息发送")
            return

        try:
            content, template = self.__build_content(text, image)
            sc_url = "http://www.pushplus.plus/send"
            event_info = {
                "token": self._token,
                "title": title,
                "content": content,
                "template": template,
                "channel": "wechat"
            }
            if self._istopic and self._topicid:
                event_info["topic"] = self._topicid

            if image:
                if str(image).startswith("https://"):
                    logger.info(f"PushPlus消息携带图片：{image}")
                else:
                    # 官方明确要求 https，http 的图片大概率不显示
                    logger.warn(f"PushPlus消息携带的图片不是 https 地址，"
                                f"公众号可能不显示：{image}")

            res = RequestUtils(content_type="application/json").post_res(sc_url, json=event_info)
            if res and res.status_code == 200:
                ret_json = res.json()
                code = ret_json.get('code')
                msg = ret_json.get('msg')
                if code == 200:
                    logger.info("PushPlus消息发送成功")
                else:
                    logger.warn(f"PushPlus消息发送，接口返回失败，错误码：{code}，错误原因：{msg}")
            elif res is not None:
                logger.warn(f"PushPlus消息发送失败，错误码：{res.status_code}，错误原因：{res.reason}")
            else:
                logger.warn("PushPlus消息发送失败，未获取到返回信息")
        except Exception as msg_e:
            logger.error(f"PushPlus消息发送异常，{str(msg_e)}")

    @staticmethod
    def __build_content(text: str, image: str = None):
        """
        组装 PushPlus 的 content 与 template

        - 没有图片时沿用 txt 模板：原样展示，换行不会被折叠
        - 有图片时切到 html 模板（txt 是纯文本模板，img 标签不保证渲染），
          并把换行转成 <br/>，否则 html 下 \\n 会被折叠成空格
        - 正文做转义，避免文本里的 & < > 把整个 html 撑坏
        """
        body = "" if text is None else str(text)
        if not image:
            return body, "txt"

        safe = (body.replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                    .replace("\n", "<br/>"))
        img = f"<img src='{image}' style='max-width:100%'/>"
        return (f"{safe}<br/>{img}" if safe else img), "html"

    def stop_service(self):
        """
        退出插件
        """
        pass