import time
from datetime import datetime, timedelta
from pathlib import Path
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
    FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
    # 插件名称
    plugin_name = "ANiStrmPro"
    # 插件描述
    plugin_desc = "自动获取当季所有番剧，免去下载，轻松拥有一个番剧媒体库（提供镜像配置，默认官方地址，增强URL兼容性）"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/shanhai2333/MoviePilot-Plugins/main/icons/anistrmpro.png"
    # 插件版本
    plugin_version = "2.8.111"  # 版本号升级，表示融合了新功能
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
    _image_url = ''
    _image_rss_url = ''
    # 任务执行间隔
    _cron = None
    _onlyonce = False
    _fulladd = False
    _selected_seasons: List[str] = []
    _storageplace = None
    _filename_remove = ''
    _date = None  # 存储当前处理的日期字符串

    # 定时器
    _scheduler: Optional[BackgroundScheduler] = None

    def _get_base_url(self) -> str:
        if self._image_url and self._image_url.strip():
            return self._image_url.strip().rstrip('/')
        return 'https://openani.an-i.workers.dev'

    def _get_rss_url(self) -> str:
        if self._image_rss_url and self._image_rss_url.strip():
            return self._image_rss_url.strip()
        return 'https://api.ani.rip/ani-download.xml'

    def _is_mirror_mode(self) -> bool:
        return bool(self._image_url and self._image_url.strip())

    def init_plugin(self, config: dict = None):
        # 停止现有任务
        self.stop_service()

        if config:
            self._enabled = config.get("enabled")
            self._image_url = config.get("image_url")
            self._image_rss_url = config.get("image_rss_url")
            self._cron = config.get("cron")
            self._onlyonce = config.get("onlyonce")
            self._fulladd = config.get("fulladd")
            if "selected_seasons" in config:
                self._selected_seasons = config.get("selected_seasons") or []
            else:
                self._selected_seasons = ["latest"]
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
        remote_season = self._get_latest_remote_season()
        if remote_season:
            self._date = remote_season
            logger.info(f'使用远端最新季度：{remote_season}')
            return remote_season

        return self._get_local_season(idx_month=idx_month)

    def _get_local_season(self, idx_month: int = None) -> str:
        current_date = datetime.now()
        current_year = current_date.year
        current_month = idx_month if idx_month else current_date.month
        season_month = ((current_month - 1) // 3) * 3 + 1
        self._date = f'{current_year}-{season_month}'
        logger.info(f"远端季度获取失败，回退使用本地时间：{current_year}年{current_month}月，对应季度 {self._date}")
        return self._date

    def _get_latest_remote_season(self) -> Optional[str]:
        payload = self._fetch_folder_payload(f'{self._get_base_url()}/')
        return self._extract_latest_season(payload.get('files') or [])

    def _get_target_seasons(self) -> List[str]:
        if self._selected_seasons:
            seasons: List[str] = []
            for season in self._selected_seasons:
                if season == "latest":
                    latest = self.__get_ani_season()
                    if latest:
                        seasons.append(latest)
                else:
                    seasons.append(season)
            return list(dict.fromkeys(seasons))
        return []

    @staticmethod
    def _extract_latest_season(files: List[Dict[str, str]]) -> Optional[str]:
        seasons: List[Tuple[int, int]] = []
        for file_info in files:
            name = file_info.get('name') or ''
            mime_type = file_info.get('mimeType') or ''
            if mime_type != ANiStrmPro.FOLDER_MIME_TYPE:
                continue

            parts = name.split('-', 1)
            if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
                continue

            seasons.append((int(parts[0]), int(parts[1])))

        if not seasons:
            return None

        year, month = max(seasons)
        return f'{year}-{month}'

    @retry(Exception, tries=3, logger=logger, ret=[])
    def _fetch_folder_payload(self, url: str) -> Dict[str, Any]:
        logger.info(f"请求季度列表：{url}")

        headers = {
            "Content-Type": "application/json"
        }
        rep = RequestUtils(
            ua=settings.USER_AGENT if settings.USER_AGENT else None,
            proxies=settings.PROXY if settings.PROXY else None,
            headers=headers
        ).post(
            url=url,
            data='{"password":"null"}'
        )

        if not rep:
            raise ValueError(f"目录请求失败：{url}")

        logger.debug(f"响应内容: {rep.text}")

        try:
            if rep.status_code != 200:
                raise ValueError(f"请求失败，状态码: {rep.status_code}, 内容: {rep.text}")
            return rep.json()
        finally:
            rep.close()

    def _collect_season_entries(self, folder_path: str, relative_dir: str = "") -> List[Dict[str, str]]:
        base_url = self._get_base_url()
        payload = self._fetch_folder_payload(f'{base_url}/{folder_path}')
        entries: List[Dict[str, str]] = []

        for file_info in payload.get('files', []):
            name = file_info.get('name') or ''
            if not name:
                continue

            mime_type = file_info.get('mimeType') or ''
            if mime_type == self.FOLDER_MIME_TYPE:
                child_relative_dir = f'{relative_dir}/{name}'.strip('/')
                child_folder_path = f"{folder_path.rstrip('/')}/{quote(name, safe='')}/"
                entries.extend(self._collect_season_entries(child_folder_path, child_relative_dir))
                continue

            encoded_name = quote(name, safe='')
            file_url = f"{base_url}/{folder_path.rstrip('/')}/{encoded_name}"
            entries.append({
                'name': name,
                'url': file_url,
                'relative_dir': relative_dir,
            })

        return entries

    def get_current_season_list(self) -> List:
        base_url = self._get_base_url()
        season = self.__get_ani_season()
        logger.info(f"获取季度文件列表：{base_url}/{season}/")

        try:
            return self._collect_season_entries(f'{season}/')
        except Exception as e:
            logger.error(f"解析季度列表失败：{str(e)}")
            return []

    def get_season_entries(self, season: str) -> List[Dict[str, str]]:
        base_url = self._get_base_url()
        logger.info(f"获取季度文件列表：{base_url}/{season}/")

        try:
            return self._collect_season_entries(f'{season}/')
        except Exception as e:
            logger.error(f"解析季度列表失败：{str(e)}")
            return []

    def get_available_seasons(self, use_cache: bool = True) -> List[str]:
        payload = self._fetch_folder_payload(f'{self._get_base_url()}/')
        seasons = []
        for file_info in payload.get('files') or []:
            name = file_info.get('name') or ''
            mime_type = file_info.get('mimeType') or ''
            if mime_type != self.FOLDER_MIME_TYPE:
                continue
            parts = name.split('-', 1)
            if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
                continue
            seasons.append(name)
        seasons.sort(key=lambda item: tuple(map(int, item.split('-'))), reverse=True)
        return seasons

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_latest_list(self) -> List:
        addr = self._get_rss_url()

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
                if not self._is_mirror_mode():
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
        """检查 URL 是否已经是标准 mp4 直链格式"""
        return url.endswith('.mp4')

    def _convert_url_format(self, url: str) -> str:
        """
        将 URL 归一为最新版 ANiStrm 使用的 mp4 直链格式
        """
        if url.endswith('.mp4'):
            return url
        if url.endswith('.mp4?d=true'):
            return url[:-7]
        if '?d=mp4' in url:
            return url.replace('?d=mp4', '.mp4')
        if '?d=true' in url and '.mp4?d=true' not in url:
            return url.replace('?d=true', '')

        if not url.endswith('.mp4') and '?' not in url:
            return f'{url}.mp4'

        return url

    def __touch_strm_file(self, file_name, file_url: str = None, relative_dir: str = None) -> bool:
        src_url = ""

        # 过滤字幕文件 (srt, vtt, ass 等)
        if file_name.lower().endswith(('.srt', '.vtt', '.ass', '.ssa')):
            return False

        if not file_url:
            # === 全量模式 (手动构建 URL) ===
            base_url = self._get_base_url()

            # 【关键修复】：对文件名进行 URL 编码，防止特殊字符导致链接失效
            # 原版逻辑：quote(file_name, safe='')
            # 注意：file_name 通常包含扩展名，我们需要保留扩展名，但对其整体编码
            encoded_filename = quote(file_name, safe='')

            src_url = f'{base_url}/{self._date}/{encoded_filename}'

            # 调试日志
            logger.debug(f"构建全量 URL: {src_url}")
        else:
            # === 增量模式 (RSS 链接) ===
            if self._is_mirror_mode():
                # 镜像模式下直接使用 RSS/XML 中的 link，避免改写后请求失败
                src_url = file_url
            else:
                # 非镜像模式沿用标准化逻辑，统一成兼容的 mp4 直链格式
                if self._is_url_format_valid(file_url):
                    src_url = file_url
                else:
                    src_url = self._convert_url_format(file_url)
                    if src_url != file_url:
                        logger.debug(f"URL 格式已修正：{file_url} -> {src_url}")

        # 处理文件名（用于本地 .strm 文件的命名）
        # 注意：本地文件名不需要 URL 编码，但需要清洗用户配置的字符串
        clean_file_name = self.__remove_strings(file_name)

        directory = Path(self._storageplace)
        if relative_dir:
            directory = directory / relative_dir
        file_path = directory / f'{clean_file_name}.strm'

        if file_path.exists():
            logger.debug(f'strm 文件已存在：{file_path.name}')
            return False

        try:
            directory.mkdir(parents=True, exist_ok=True)
            file_path.write_text(src_url, encoding='utf-8')
            logger.debug(f'创建 strm 文件成功：{file_path.name} -> {src_url[:50]}...')
            return True
        except Exception as e:
            logger.error(f'创建 strm 源文件失败：{file_path.name} - {str(e)}, 链接：{src_url}')
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
            seasons = self._get_target_seasons()
            if not seasons:
                logger.info('未选择任何季度，全量任务结束')
                return

            for season in seasons:
                file_entries = self.get_season_entries(season)
                logger.info(f'本次处理季度 {season} 全量列表 {len(file_entries)} 个文件')
                for file_entry in file_entries:
                    if self.__touch_strm_file(file_name=file_entry['name'],
                                              file_url=file_entry.get('url'),
                                              relative_dir=file_entry.get('relative_dir')):
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
        season_options = self.__build_season_options()
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
                                    {'component': 'VSwitch', 'props': {'model': 'fulladd', 'label': '按所选季度补库'}}]
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
                                'props': {'cols': 12, 'md': 6},
                                'content': [{'component': 'VSelect',
                                             'props': {'model': 'selected_seasons', 'label': '拉取季度',
                                                       'items': season_options, 'multiple': True, 'chips': True,
                                                       'clearable': True,
                                                       'hint': '开启“按所选季度补库”后，按这里选择的季度检查并补齐 strm；已存在文件会自动跳过',
                                                       'persistent-hint': True}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
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
                                            'text': '功能说明：\n1. 自动从 ANi 抓取直链生成 strm 文件。\n2. 支持镜像配置，镜像地址留空则使用默认官方地址。\n3. 支持文件名清洗（删除特定字符串）。\n4. 支持按所选季度递归补库，自动保留子目录结构。',
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
            "storageplace": '/downloads/strm',
            "selected_seasons": ["latest"],
            "cron": "*/20 22,23,0,1 * * *",
            "filename_remove": "",
            "image_url": "",
            "image_rss_url": ""
        }

    def __build_season_options(self) -> List[Dict[str, str]]:
        try:
            seasons = self.get_available_seasons() or [self._get_local_season()]
        except Exception:
            seasons = [self._get_local_season()]
        return [{"title": "最新季", "value": "latest"}] + [
            {"title": season, "value": season} for season in seasons
        ]

    def __update_config(self):
        self.update_config({
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "enabled": self._enabled,
            "fulladd": self._fulladd,
            "storageplace": self._storageplace,
            "selected_seasons": self._selected_seasons,
            "image_url": self._image_url,
            "image_rss_url": self._image_rss_url,
            "filename_remove": self._filename_remove,
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
