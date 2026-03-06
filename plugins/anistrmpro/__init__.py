import os
import time
from datetime import datetime, timedelta
from urllib.parse import quote

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
    重试装饰器
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
    plugin_desc = "自动获取当季所有番剧，免去下载，轻松拥有一个番剧媒体库（提供镜像配置，默认官方地址，增强URL兼容性）"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/shanhai2333/MoviePilot-Plugins/main/icons/anistrmpro.png"
    # 插件版本
    plugin_version = "2.8.6"  # 版本号升级，表示融合了新功能
    # 插件作者
    plugin_author = "honue, shanhai2333, fused_by_ai"
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
    _before_month = ''
    _before_year = ''
    _storageplace = None
    _filename_remove = ''
    _date = None  # 存储当前处理的日期字符串

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
            self._filename_remove = config.get("filename_remove")

        if self._enabled or self._onlyonce:
            # 定时服务
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            if self._enabled and self._cron:
                try:
                    self._scheduler.add_job(func=self.__task,
                                            trigger=CronTrigger.from_crontab(self._cron),
                                            name="ANiStrm 文件创建")
                    logger.info(f'ANi-Strm 定时任务创建成功：{self._cron}')
                except Exception as err:
                    logger.error(f"定时任务配置错误：{str(err)}")

            if self._onlyonce:
                logger.info(f"ANi-Strm 服务启动，立即运行一次")
                self._scheduler.add_job(func=self.__task, args=[self._fulladd], trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="ANiStrm 文件创建")
                # 关闭一次性开关 全量转移
                self._onlyonce = False
                self._fulladd = False

            self.__update_config()

            # 启动任务
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __get_ani_season(self, idx_month: int = None) -> str:
        current_year = 0
        current_month = 0

        # 优先使用配置的指定时间（主要用于镜像回溯）
        if self._before_year and self._before_month:
            try:
                year_val = int(self._before_year)
                month_val = int(self._before_month)
                if 2020 <= year_val <= 2030:  # 简单校验
                    current_year = year_val
                    current_month = month_val
                    logger.info(f'使用指定时间：{current_year}年{current_month}月')
            except ValueError:
                pass

        # 如果没有指定或指定无效，使用当前时间
        if current_year == 0:
            current_date = datetime.now()
            current_year = current_date.year
            current_month = idx_month if idx_month else current_date.month

        logger.info(f"获取 {current_year}年{current_month}月 所在季度番剧信息")

        for month in range(current_month, 0, -1):
            if month in [10, 7, 4, 1]:
                self._date = f'{current_year}-{month}'
                return f'{current_year}-{month}'

        # 兜底，如果当前月还没到季度初，取上一个季度（逻辑上上面的循环应该能覆盖，但以防万一）
        # 实际上上面的循环是从 current_month 往下找第一个季度月，逻辑是正确的
        return f'{current_year}-1'  # 极端情况返回1月

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_current_season_list(self) -> List:
        base_url = self._image_url if self._use_image else 'https://openani.an-i.workers.dev'
        url = f'{base_url}/{self.__get_ani_season()}/'

        logger.info(f"请求季度列表：{url}")
        rep = RequestUtils(ua=settings.USER_AGENT if settings.USER_AGENT else None,
                           proxies=settings.PROXY if settings.PROXY else None).post(url=url)
        logger.debug(rep.text)

        try:
            files_json = rep.json()['files']
            return [file['name'] for file in files_json]
        except Exception as e:
            logger.error(f"解析季度列表失败：{str(e)}")
            return []

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_latest_list(self) -> List:
        addr = self._image_rss_url if self._use_image else 'https://api.ani.rip/ani-download.xml'

        logger.info(f"请求 RSS 列表：{addr}")
        ret = RequestUtils(ua=settings.USER_AGENT if settings.USER_AGENT else None,
                           proxies=settings.PROXY if settings.PROXY else None).get_res(addr)
        if not ret or not ret.text:
            return []

        ret_xml = ret.text
        ret_array = []

        # 解析 XML
        try:
            dom_tree = xml.dom.minidom.parseString(ret_xml)
            rootNode = dom_tree.documentElement
            items = rootNode.getElementsByTagName("item")
            for item in items:
                rss_info = {}
                title = DomUtils.tag_value(item, "title", default="")
                link = DomUtils.tag_value(item, "link", default="")

                if not title or not link:
                    continue

                rss_info['title'] = title

                # 如果不是镜像模式，替换域名
                if not self._use_image:
                    link = link.replace("resources.ani.rip", "openani.an-i.workers.dev")

                rss_info['link'] = link
                ret_array.append(rss_info)
        except Exception as e:
            logger.error(f"解析 RSS XML 失败：{str(e)}")

        return ret_array

    def __remove_strings(self, file_name: str) -> str:
        """
        从文件名中删除配置的字符串
        """
        if not self._filename_remove:
            return file_name

        remove_list = self._filename_remove.split('@')
        for remove_str in remove_list:
            remove_str = remove_str.strip()
            if remove_str:
                file_name = file_name.replace(remove_str, '')

        return file_name

    def _is_url_format_valid(self, url: str) -> bool:
        """检查 URL 格式是否符合要求（.mp4?d=true）"""
        return url.endswith('.mp4?d=true')

    def _convert_url_format(self, url: str) -> str:
        """
        将 URL 转换为符合要求的格式 (.mp4?d=true)
        移植自原版 ANiStrm，增强兼容性
        """
        if '?d=mp4' in url:
            # 将 ?d=mp4 替换为 .mp4?d=true
            return url.replace('?d=mp4', '.mp4?d=true')
        elif url.endswith('.mp4'):
            # 如果已经以.mp4结尾，添加?d=true
            return f'{url}?d=true'
        elif '.mp4?' in url:
            # 已经有.mp4且有参数，但不是?d=true，可能是?d=1之类，视情况处理
            # 这里简单处理，如果包含.mp4?但不以?d=true结尾，尝试替换参数部分
            # 这种比较少见，暂不处理，直接返回或按需修改
            pass

        # 如果既不是标准格式，也不包含常见变体，为了保险起见，
        # 如果看起来像直链但没有后缀，尝试追加（针对某些特殊 API 返回）
        # 但大多数 RSS 链接都是完整的。如果不确定，保持原样可能比改错好。
        # 不过为了匹配原版逻辑，如果完全不符合，我们假设它可能需要后缀
        if not url.endswith('.mp4') and '?' not in url:
            return f'{url}.mp4?d=true'

        return url

    def __touch_strm_file(self, file_name, file_url: str = None) -> bool:
        src_url = ""

        # 过滤字幕文件 (srt, vtt, ass 等)
        if file_name.lower().endswith(('.srt', '.vtt', '.ass', '.ssa')):
            return False

        if not file_url:
            # === 全量模式 (手动构建 URL) ===
            base_url = self._image_url if self._use_image else 'https://openani.an-i.workers.dev'

            # 【关键修复】：对文件名进行 URL 编码，防止特殊字符导致链接失效
            # 原版逻辑：quote(file_name, safe='')
            # 注意：file_name 通常包含扩展名，我们需要保留扩展名，但对其整体编码
            encoded_filename = quote(file_name, safe='')

            src_url = f'{base_url}/{self._date}/{encoded_filename}?d=true'

            # 调试日志
            logger.debug(f"构建全量 URL: {src_url}")
        else:
            # === 增量模式 (RSS 链接) ===
            # RSS 返回的链接通常已经是编码过的，或者由 API 保证合法性
            # 但我们依然应用格式转换逻辑 (.mp4?d=true)
            if self._is_url_format_valid(file_url):
                src_url = file_url
            else:
                # 转换格式前，理论上不需要再次编码，因为 link 来自 XML 通常是完整的
                # 但如果 link 中包含未编码的空格，_convert_url_format 可能会处理不当
                # 这里保持原样，信任 RSS 源提供的链接完整性，只做后缀修正
                src_url = self._convert_url_format(file_url)
                if src_url != file_url:
                    logger.debug(f"URL 格式已修正：{file_url} -> {src_url}")

        # 处理文件名（用于本地 .strm 文件的命名）
        # 注意：本地文件名不需要 URL 编码，但需要清洗用户配置的字符串
        clean_file_name = self.__remove_strings(file_name)

        file_path = f'{self._storageplace}/{clean_file_name}.strm'

        if os.path.exists(file_path):
            logger.debug(f'strm 文件已存在：{clean_file_name}.strm')
            return False

        try:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(src_url)
                logger.debug(f'创建 strm 文件成功：{clean_file_name}.strm -> {src_url[:50]}...')
                return True
        except Exception as e:
            # 修复 logger 格式化
            logger.error(f'创建 strm 源文件失败：{str(e)}, 链接：{src_url}')
            return False

    def __task(self, fulladd: bool = False):
        cnt = 0
        if not fulladd:
            # 增量模式
            rss_info_list = self.get_latest_list()
            logger.info(f'本次处理增量更新 {len(rss_info_list)} 个文件')
            for rss_info in rss_info_list:
                rss_link = rss_info.get('link')
                if rss_link:
                    if self.__touch_strm_file(file_name=rss_info['title'], file_url=rss_link):
                        cnt += 1
        else:
            # 全量模式
            name_list = self.get_current_season_list()
            logger.info(f'本次处理全量列表 {len(name_list)} 个文件')
            for file_name in name_list:
                if self.__touch_strm_file(file_name=file_name):
                    cnt += 1

        logger.info(f'任务完成，新创建了 {cnt} 个 strm 文件')

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {'component': 'VSwitch', 'props': {'model': 'fulladd', 'label': '下次全量创建'}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {'component': 'VSwitch', 'props': {'model': 'use_image', 'label': '使用镜像'}}]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField', 'props': {'model': 'cron', 'label': '执行周期',
                                                                                  'placeholder': '*/20 22,23,0,1 * * *'}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField',
                                             'props': {'model': 'storageplace', 'label': 'Strm 存储地址',
                                                       'placeholder': '/downloads/strm'}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField',
                                             'props': {'model': 'filename_remove', 'label': '文件名删除字符串 (@分隔)',
                                                       'placeholder': 'ABC@DEF'}}]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'v_if': 'use_image',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{'component': 'VTextField',
                                             'props': {'model': 'before_year', 'label': '指定年份',
                                                       'placeholder': '2024'}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{'component': 'VTextField',
                                             'props': {'model': 'before_month', 'label': '指定月份',
                                                       'placeholder': '4'}}]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'v_if': 'use_image',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{'component': 'VTextField',
                                             'props': {'model': 'image_url', 'label': '镜像地址',
                                                       'placeholder': 'https://ani.v300.eu.org'}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{'component': 'VTextField',
                                             'props': {'model': 'image_rss_url', 'label': '镜像 RSS 地址',
                                                       'placeholder': 'https://aniapi.v300.eu.org/ani-download.xml'}}]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '功能说明：\n1. 自动从 ANi 抓取直链生成 strm 文件。\n2. 支持镜像配置，解决访问问题。\n3. 支持文件名清洗（删除特定字符串）。\n4. 增强版：自动修正 RSS 链接格式，确保播放器兼容性。',
                                            'style': 'white-space: pre-line;'
                                        }
                                    },
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'warning',
                                            'variant': 'tonal',
                                            'text': '注意：\n- Emby/Jellyfin 容器需配置 http_proxy 环境变量。\n- 文件名删除字符串用 @ 分隔，例如："ANSUB@NC-Raw"',
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
            "before_month": "",
            "before_year": "",
            "storageplace": '/downloads/strm',
            "cron": "*/20 22,23,0,1 * * *",
            "filename_remove": "",
            "image_url": "",
            "image_rss_url": ""
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
            "image_rss_url": self._image_rss_url,
            "filename_remove": self._filename_remove,
            "before_month": self._before_month,
            "before_year": self._before_year
        })

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error("退出插件失败：%s" % str(e))


if __name__ == "__main__":
    # 测试用例
    pro = ANiStrmPro()
    # 模拟配置
    pro.init_plugin({
        "enabled": True,
        "use_image": False,
        "storageplace": "/tmp/strm_test"
    })
    # 测试 URL 转换逻辑
    test_urls = [
        "http://test/file.mp4",
        "http://test/file?d=mp4",
        "http://test/file.mp4?d=true",
        "http://test/file"
    ]
    for u in test_urls:
        print(f"Original: {u} -> Converted: {pro._convert_url_format(u)}")