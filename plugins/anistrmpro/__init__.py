import os
import time
from datetime import datetime, timedelta

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.utils.http import RequestUtils
from app.core.config import settings
from app.plugins import _PluginBase
from typing import Any, List, Dict, Tuple, Optional
from app.log import logger
import xml.dom.minidom
from app.utils.dom import DomUtils


def retry(ExceptionToCheck: Any,
          tries: int = 3, delay: int = 3, backoff: int = 1, logger: Any = None, ret: Any = None):
    """
    :param ExceptionToCheck: 需要捕获的异常
    :param tries: 重试次数
    :param delay: 延迟时间
    :param backoff: 延迟倍数
    :param logger: 日志对象
    :param ret: 默认返回
    """

    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = f"未获取到文件信息，{mdelay}秒后重试 ..."
                    if logger:
                        logger.warn(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            if logger:
                logger.warn('请确保当前季度番剧文件夹存在或检查网络问题')
            return ret

        return f_retry

    return deco_retry


class ANiStrmPro(_PluginBase):
    # 插件名称
    plugin_name = "ANiStrmPro"
    # 插件描述
    plugin_desc = "自动获取当季所有番剧，免去下载，轻松拥有一个番剧媒体库（提供镜像配置，默认官方地址）"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/shanhai2333/MoviePilot-Plugins/main/icons/anistrmpro.png"
    # 插件版本
    plugin_version = "1.7"
    # 插件作者
    plugin_author = "shanhai2333"
    # 作者主页
    author_url = "https://github.com/shanhai2333"
    # 插件配置项ID前缀
    plugin_config_prefix = "anistrmpro_"
    # 加载顺序
    plugin_order = 15
    # 可使用的用户级别
    auth_level = 2

    # 页面配置属性
    _enabled = False
    _use_image = False
    _image_url = ''
    _image_rss_url = ''
    # 任务执行间隔
    _cron = None
    _onlyonce = False
    _fulladd = False
    _before_month = 0
    _before_year = 0
    _storageplace = None

    # 定时器
    _scheduler: Optional[BackgroundScheduler] = None

    def init_plugin(self, config: dict = None):
        # 停止现有任务
        self.stop_service()

        if config:
            self._enabled = config.get("enabled")
            self._use_image = config.get("use_image")
            self._image_url = config.get("image_url")
            self._image_rss_url = config.get("image_rss_url")
            self._cron = config.get("cron")
            self._onlyonce = config.get("onlyonce")
            self._fulladd = config.get("fulladd")
            self._before_month = config.get("before_month")
            self._before_year = config.get("before_year")
            self._storageplace = config.get("storageplace")
            # 加载模块
        if self._enabled or self._onlyonce:
            # 定时服务
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            if self._enabled and self._cron:
                try:
                    self._scheduler.add_job(func=self.__task,
                                            trigger=CronTrigger.from_crontab(self._cron),
                                            name="ANiStrm文件创建")
                    logger.info(f'ANi-Strm定时任务创建成功：{self._cron}')
                except Exception as err:
                    logger.error(f"定时任务配置错误：{str(err)}")

            if self._onlyonce:
                logger.info(f"ANi-Strm服务启动，立即运行一次")
                self._scheduler.add_job(func=self.__task, args=[self._fulladd], trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="ANiStrm文件创建")
                # 关闭一次性开关 全量转移
                self._onlyonce = False
                self._fulladd = False
            self.__update_config()

            # 启动任务
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __get_ani_season(self, idx_month: int = None) -> str:
        if self._before_month != 0 & self._before_year !=0:
            current_year = self._before_year
            current_month = self._before_month
        else:
            current_date = datetime.now()
            current_year = current_date.year
            current_month = idx_month if idx_month else current_date.month
        for month in range(current_month, 0, -1):
            if month in [10, 7, 4, 1]:
                self._date = f'{current_year}-{month}'
                return f'{current_year}-{month}'

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_current_season_list(self) -> List:
        if not self._use_image:
            url = f'https://aniopen.an-i.workers.dev/{self.__get_ani_season()}/'
        else:
            url = f'{self._image_url}/{self.__get_ani_season()}/'

        rep = RequestUtils(ua=settings.USER_AGENT if settings.USER_AGENT else None,
                           proxies=settings.PROXY if settings.PROXY else None).post(url=url)
        logger.debug(rep.text)
        files_json = rep.json()['files']
        return [file['name'] for file in files_json]

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_latest_list(self) -> List:
        if not self._use_image:
            addr = 'https://api.ani.rip/ani-download.xml'
        else:
            addr = self._image_rss_url
        ret = RequestUtils(ua=settings.USER_AGENT if settings.USER_AGENT else None,
                           proxies=settings.PROXY if settings.PROXY else None).get_res(addr)
        ret_xml = ret.text
        ret_array = []
        # 解析XML
        dom_tree = xml.dom.minidom.parseString(ret_xml)
        rootNode = dom_tree.documentElement
        items = rootNode.getElementsByTagName("item")
        for item in items:
            rss_info = {}
            # 标题
            title = DomUtils.tag_value(item, "title", default="")
            # 链接
            link = DomUtils.tag_value(item, "link", default="")
            rss_info['title'] = title
            if not self._use_image:
                rss_info['link'] = link.replace("resources.ani.rip", "aniopen.an-i.workers.dev")
            ret_array.append(rss_info)
        return ret_array

    def __touch_strm_file(self, file_name, file_url: str = None) -> bool:
        if not file_url:
            if not self._use_image:
                src_url = f'https://aniopen.an-i.workers.dev/{self._date}/{file_name}?d=true'
            else:
                # 以.拆分file_name，最后为拓展名，srt和vtt忽略
                file_name_split = file_name.split('.')
                if not (file_name_split[-1] in ['srt', 'vtt']):
                    src_url = f'{self._image_url}/{self._date}/{file_name}?d=true'
        else:
            src_url = file_url
        file_path = f'{self._storageplace}/{file_name}.strm'
        if os.path.exists(file_path):
            logger.debug(f'{file_name}.strm 文件已存在')
            return False
        try:
            with open(file_path, 'w') as file:
                file.write(src_url)
                logger.debug(f'创建 {file_name}.strm 文件成功')
                return True
        except Exception as e:
            logger.error('创建strm源文件失败：' + str(e))
            return False

    def __task(self, fulladd: bool = False):
        cnt = 0
        # 增量添加更新
        if not fulladd:
            rss_info_list = self.get_latest_list()
            logger.info(f'本次处理 {len(rss_info_list)} 个文件')
            for rss_info in rss_info_list:
                if self.__touch_strm_file(file_name=rss_info['title'], file_url=rss_info['link']):
                    cnt += 1
        # 全量添加当季
        else:
            name_list = self.get_current_season_list()
            logger.info(f'本次处理 {len(name_list)} 个文件')
            for file_name in name_list:
                if self.__touch_strm_file(file_name=file_name):
                    cnt += 1
        logger.info(f'新创建了 {cnt} 个strm文件')

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
                                    'md': 4
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
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'fulladd',
                                            'label': '下次创建当前季度所有番剧strm',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'use_image',
                                            'label': '使用镜像',
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
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期',
                                            'placeholder': '0 0 ? ? ?'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'storageplace',
                                            'label': 'Strm存储地址',
                                            'placeholder': '/downloads/strm'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    # 增加一行输入年月
                    {
                        'component': 'VRow',
                        'v_if': 'use_image',
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
                                            'model': 'before_year',
                                            'label': '年',
                                            'placeholder': '最少2020'
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
                                            'model': 'before_month',
                                            'label': '月',
                                            'placeholder': '2020年时最少4'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    # 增加一行输入镜像地址 和  镜像xml下载地址
                    {
                        'component': 'VRow',
                        'v_if': 'use_image',
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
                                            'model': 'image_url',
                                            'label': '镜像地址',
                                            'placeholder': 'https://ani.v300.eu.org'
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
                                            'model': 'image_rss_url',
                                            'label': '镜像RSS地址',
                                            'placeholder': 'https://aniapi.v300.eu.org/ani-download.xml'
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
                                            'text': '自动从open ANi抓取下载直链生成strm文件，免去人工订阅下载' + '\n' +
                                                    '配合目录监控使用，strm文件创建在/downloads/strm' + '\n' +
                                                    '通过目录监控转移到link媒体库文件夹 如/downloads/link/strm  mp会完成刮削',
                                            'style': 'white-space: pre-line;'
                                        }
                                    },
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': 'emby容器需要设置代理，docker的环境变量必须要有http_proxy代理变量，大小写敏感，具体见readme.' + '\n' +
                                                    'https://github.com/honue/MoviePilot-Plugins',
                                            'style': 'white-space: pre-line;'
                                        }
                                    },
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '镜像部署教程查看X-yael大佬的博客' + '\n' +
                                                    'https://blog.x-yael.eu.org/p/ani/',
                                            'style': 'white-space: pre-line;'
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
            "onlyonce": False,
            "fulladd": False,
            "use_image": False,
            "before_month": "0",
            "before_year": "0",
            "storageplace": '/downloads/strm',
            "cron": "*/20 22,23,0,1 * * *",
        }

    def __update_config(self):
        self.update_config({
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "enabled": self._enabled,
            "fulladd": self._fulladd,
            "storageplace": self._storageplace,
            "use_image": self._use_image,
            "image_url": self._image_url,
            "image_rss_url": self._image_rss_url
        })

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error("退出插件失败：%s" % str(e))


if __name__ == "__main__":
    anistrmpro = ANiStrmPro()
    name_list = anistrmpro.get_latest_list()
    print(name_list)