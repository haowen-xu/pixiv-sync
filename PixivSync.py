import json
import os
import codecs
import re
import shutil
import subprocess
import sys
import threading
import urllib
from dataclasses import dataclass
from datetime import datetime
from typing import *
from urllib.request import urlopen

import click
import scrapy
import yaml
from scrapy.crawler import CrawlerProcess
from scrapy.http import Request, Response

__version__ = '0.0.1'
PIXIV_TOKEN_COOKIE_NAME = 'PHPSESSID'


def html_to_text(html: str):
    html = BR_PATTERN.sub('\n', html)
    return html

BR_PATTERN = re.compile(r'<br[^<>]*>', re.I)


class SyncDB(object):

    path: str
    data: Dict[str, Any]
    lock: threading.RLock

    def __init__(self, path: str):
        if os.path.exists(path):
            with codecs.open(path, 'rb', 'utf-8') as f:
                cnt = f.read()
            data = json.loads(cnt)
            if not isinstance(data, dict):
                raise IOError(f'DB malformed: {path}')
        else:
            data = {}

        for key in ('illusts', 'users'):
            if key not in data:
                data[key] = {}
            elif not isinstance(data[key], dict):
                raise IOError(f'DB malformed: {path}')

        self.path = os.path.abspath(path)
        self.data = data
        self.lock = threading.RLock()

    def save(self, max_backup: int = 10):
        with self.lock:
            output_content = json.dumps(self.data)

            if os.path.exists(self.path):
                parent_dir, file_name = os.path.split(self.path)

                # move the previous db to a new backup
                new_suffix = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
                new_backup_name = f'{file_name}-{new_suffix}'
                shutil.move(self.path, os.path.join(parent_dir, new_backup_name))

                # cleanup old backups
                backup_list = []
                e_prefix = f'{file_name}-'
                for e in os.listdir(parent_dir):
                    if e.startswith(e_prefix) and e != file_name:
                        backup_list.append(e)
                backup_list.sort()
                for old_backup in backup_list[:len(backup_list) - max_backup]:
                    os.remove(os.path.join(parent_dir, old_backup))

            with codecs.open(self.path, 'wb', 'utf-8') as f:
                f.write(output_content)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.save()

    # ---- common get/set items ----
    def __getitem__(self, key: str):
        with self.lock:
            return self.data[key]

    def __setitem__(self, key: str, val):
        with self.lock:
            self.data[key] = val

    def get(self, key: str, default=None):
        with self.lock:
            return self.data.get(key, default)

    def get_cookies(self, default=None):
        return self.get('cookies', default)

    def set_cookies(self, val: Dict[str, str]):
        self['cookies'] = val

    def get_illust_ids(self):
        with self.lock:
            return list(self.get('illusts', {}))

    # ---- read/write nested collections ----
    def _get_dict(self, coll: str, id: str, default=None):
        with self.lock:
            return self.data[coll].get(id, default)

    def _update_dict(self, coll: str, id: str, val: Dict[str, Any]):
        with self.lock:
            if id in self.data[coll]:
                self.data[coll][id].update(val)
            else:
                self.data[coll][id] = dict(val)

    def get_illust(self, illust_id: str, default=None):
        return self._get_dict('illusts', illust_id, default)

    def update_illust(self, illust_id: str, val: Dict[str, Any]):
        self._update_dict('illusts', illust_id, val)

    def set_illust_fetched(self, illust_id: str, image_id: int):
        with self.lock:
            self['illusts'][illust_id]['images'][image_id]['fetched'] = True

    def get_user(self, user_id: str, default=None):
        return self._get_dict('users', user_id, default)

    def update_user(self, user_id: str, val: Dict[str, Any]):
        self._update_dict('users', user_id, val)


class BasePixivSpider(scrapy.Spider):

    DEFAULT_HEADERS: Dict[str, str] = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/apng,*/*;q=0.8,v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
    }

    config: Dict[str, any]
    sync_db: SyncDB
    headers: Dict[str, Any]
    cookies: Dict[str, Any]

    def __init__(self, config: Dict[str, Any], sync_db: SyncDB):
        super().__init__(name=self.__class__.__qualname__)
        self.config = config
        self.sync_db = sync_db
        self.headers = dict(config.get('http.headers') or ())
        self.cookies = dict(sync_db.get_cookies() or ())

        for k, v in self.DEFAULT_HEADERS.items():
            self.headers.setdefault(k, v)


class PixivMetaSpider(BasePixivSpider):

    AUTHOR_ID_PATTERNS = [
        re.compile(r'^(\d+)$'),
        re.compile(r'^https?://www\.pixiv\.net/users/(\d+)(?:/.*)?')
    ]
    ILLUST_ID_PATTERNS = [
        re.compile(r'/artworks/(\d+)(?:/.*)?$'),
    ]

    start_author_ids: List[str]

    def __init__(self, config: Dict[str, Any], sync_db: SyncDB, full_sync: bool):
        super().__init__(config, sync_db)

        self.start_author_ids = []
        self.favourites = list(config.get('favourites', []))
        self.full_sync = full_sync

        for author_id_or_url in config.get('authors', []):
            author_id = None
            for pattern in self.AUTHOR_ID_PATTERNS:
                m = pattern.match(author_id_or_url)
                if m:
                    author_id = m.group(1)
                    break
            if author_id is None:
                raise ValueError(f'No author ID can be recognized from: '
                                 f'{author_id_or_url}')
            self.start_author_ids.append(author_id)

    def start_requests(self):
        for fav in self.favourites:
            if fav in ['public', 'private']:
                rest = {'public': 'show', 'private': 'hide'}[fav]
                yield scrapy.Request(
                    url=f'https://www.pixiv.net/bookmark.php?rest={rest}&p=1',
                    callback=self.parse_favourite_page,
                    headers=self.headers,
                    cookies=self.cookies,
                    meta={'rest': rest, 'p': 1}
                )

        for author_id in self.start_author_ids:
            yield scrapy.Request(
                url=f'https://www.pixiv.net/ajax/user/{author_id}/profile/all',
                callback=self.parse_author_profile_ajax,
                headers=self.headers,
                cookies=self.cookies,
                meta={'author_id': author_id}
            )

    def _make_illust_requests(self, illust_id: str):
        illust_item = self.sync_db.get_illust(illust_id, {})
        if 'title' not in illust_item:
            yield Request(
                url=f'https://www.pixiv.net/artworks/{illust_id}',
                callback=self.parse_illust_page,
                headers=self.headers,
                cookies=self.cookies,
                meta={'illust_id': illust_id}
            )
        if 'images' not in illust_item:
            yield Request(
                url=f'https://www.pixiv.net/ajax/illust/{illust_id}/pages',
                callback=self.parse_illust_images_ajax,
                headers=self.headers,
                cookies=self.cookies,
                meta={'illust_id': illust_id},
            )

    def parse_favourite_page(self, response: Response):
        rest, p = response.meta['rest'], response.meta['p']
        links = response.css('div.display_editable_works li.image-item > a.work ::attr(href)')
        new_count = total_count = 0

        for illust_link in links:
            illust_url = illust_link.get()
            illust_id = None
            for illust_id_pattern in self.ILLUST_ID_PATTERNS:
                m = illust_id_pattern.search(illust_url)
                if m:
                    illust_id = m.group(1)
                    break
            if illust_id is not None:
                requests = list(self._make_illust_requests(illust_id))
                if requests:
                    new_count += 1
                    yield from requests

            total_count += 1

        if new_count > 0 or (self.full_sync and total_count > 0):
            yield scrapy.Request(
                url=f'https://www.pixiv.net/bookmark.php?rest={rest}&p={p+1}',
                callback=self.parse_favourite_page,
                headers=self.headers,
                cookies=self.cookies,
                meta={'rest': rest, 'p': p+1}
            )

    def parse_author_profile_ajax(self, response: Response):
        author_id = response.meta['author_id']
        content = json.loads(response.body_as_unicode())
        if content['error']:
            raise RuntimeError(
                f'Failed to load illusts of author {author_id}: '
                f'{content["message"]}'
            )

        illusts = content.get('body', {}).get('illusts', [])
        for illust_id in illusts:
            yield from self._make_illust_requests(illust_id)

    def parse_illust_page(self, response: Response):
        illust_id = response.meta['illust_id']
        preload_data = response.css('#meta-preload-data ::attr(content)')[0].get()
        preload_data = json.loads(preload_data)
        illust_data = preload_data['illust'][illust_id]

        def filter_dict(d):
            return {k: v for k, v in d.items() if v}

        def get_tags():
            tags = []
            for t in illust_data.get('tags', {}).get('tags', []):
                t_name = t.get('tag')
                if not t_name:
                    continue
                t_romaji = t.get('romaji')
                t_translation = t.get('translation', {}).get('en', '')
                tags.append(filter_dict({
                    'name': t_name,
                    'romaji': t_romaji,
                    'translation': t_translation
                }))
            return tags

        item = filter_dict({
            'id': illust_data['id'],
            'title': illust_data['title'],
            'raw_description': illust_data.get('description', ''),
            'description': html_to_text(illust_data.get('description', '')),
            'create_time': illust_data.get('createDate', ''),
            'update_time': illust_data.get('updateDate', ''),
            'author_id': illust_data.get('userId', ''),
            'author_name': illust_data.get('userName', ''),
            'tags': get_tags()
        })
        if item:
            self.sync_db.update_illust(illust_id, item)

    def parse_illust_images_ajax(self, response: Response):
        illust_id = response.meta['illust_id']
        content = json.loads(response.body_as_unicode())
        if content['error']:
            raise RuntimeError(
                f'Failed to load images of illust {illust_id}: '
                f'{content["message"]}'
            )

        images = []
        for image in content.get('body', []):
            images.append({
                'url': image['urls']['original'],
                'width': image['width'],
                'height': image['height'],
            })

        with self.sync_db.lock:
            self.sync_db.update_illust(illust_id, {'images': images})


@dataclass
class FetchImageJob(object):
    file_path: str
    image_url: str
    illust_id: str
    image_id: int


class PixivImagesSpider(BasePixivSpider):

    image_jobs: List[FetchImageJob]

    def __init__(self,
                 config: Dict[str, Any],
                 sync_db: SyncDB,
                 image_jobs: Iterable[FetchImageJob]):
        super().__init__(config, sync_db)
        self.image_jobs = list(image_jobs)

    def start_requests(self):
        for image_job in self.image_jobs:
            headers = dict(self.headers)
            headers['Referer'] = f'https://www.pixiv.net/artworks/{image_job.illust_id}'
            request = scrapy.Request(
                url=image_job.image_url,
                callback=self.parse,
                headers=headers,
                cookies=self.cookies,
                meta={'job': image_job}
            )
            yield request

    def parse(self, response: Response):
        job: FetchImageJob = response.meta['job']
        parent_dir = os.path.split(job.file_path)[0]
        if not os.path.isdir(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
        cnt = response.body
        try:
            with open(job.file_path, 'wb') as f:
                f.write(cnt)
        except Exception:
            if os.path.exists(job.file_path):
                try:
                    os.remove(job.file_path)
                except Exception:
                    pass
            raise
        self.sync_db.set_illust_fetched(job.illust_id, job.image_id)


def load_config_file(config_file: str) -> Dict[str, Any]:
    if os.path.exists(config_file):
        with codecs.open(config_file, 'rb', 'utf-8') as f:
            cnt = f.read()
        if not cnt.strip():
            return {}
        data = yaml.load(cnt, Loader=yaml.SafeLoader)
        if not isinstance(data, dict):
            raise IOError(f'Config file malformed: {config_file}')
        return data


def get_logged_in_user_id(cookies):
    headers = dict(BasePixivSpider.DEFAULT_HEADERS)
    headers['Cookie'] = f'PHPSESSID={cookies["PHPSESSID"]}'
    headers.pop('Accept-Encoding')

    req = urllib.request.Request(
        'https://www.pixiv.net', method='GET', headers=headers)
    with urlopen(req) as resp:
        cnt = resp.read().decode('utf-8')

    if re.search(r'pixiv\.user\.loggedIn\s*=\s*true', cnt) is None:
        raise RuntimeError('Login token is invalid. You may login again.')
    return re.search(r'pixiv\.user\.id\s*=\s*"(\d+)"', cnt).group(1)


@click.group()
def pixiv_sync():
    """Pixiv illustrations sync tool."""


@pixiv_sync.command()
@click.option('-C', '--config-file', help='The YAML config file.',
              default='config.yml', required=True)
@click.argument('token', required=True)
def set_token(config_file, token):
    # validate the token
    cookies = {PIXIV_TOKEN_COOKIE_NAME: token}
    _ = get_logged_in_user_id(cookies)

    # set the token
    config = load_config_file(config_file)
    parent_dir = os.path.split(os.path.abspath(config['sync.db']))[0]
    if not os.path.isdir(parent_dir):
        os.makedirs(parent_dir, exist_ok=True)
    sync_db = SyncDB(config['sync.db'])
    with sync_db:
        sync_db.set_cookies(cookies)


@pixiv_sync.command()
@click.option('-C', '--config-file', help='The YAML config file.',
              default='config.yml', required=True)
@click.option('--full-sync', is_flag=True, default=False)
def sync_list(config_file, full_sync):
    """Synchronize the illustration list."""
    config = load_config_file(config_file)
    sync_db = SyncDB(config['sync.db'])
    with sync_db:
        print('Fetching new illustrations list ...')
        process = CrawlerProcess()
        process.crawl(PixivMetaSpider, config, sync_db, full_sync)
        process.start()


@pixiv_sync.command()
@click.option('-C', '--config-file', help='The YAML config file.',
              default='config.yml', required=True)
def sync_images(config_file):
    """Synchronize the illustration images."""
    config = load_config_file(config_file)
    sync_db = SyncDB(config['sync.db'])
    download_dir = os.path.abspath(config['download.dir'])

    with sync_db:
        image_jobs: List[FetchImageJob] = []
        for illust_id in sync_db.get_illust_ids():
            illust = sync_db.get_illust(illust_id, {})
            author_name = illust['author_name']
            parent_dir = os.path.join(download_dir, author_name)

            images = illust.get('images', [])
            if len(images) > 1:
                parent_dir = os.path.join(parent_dir, illust_id)
            for i, image in enumerate(
                    sync_db.get_illust(illust_id, {}).get('images')):
                if image.get('fetched', False):
                    continue
                image_url = image['url']
                file_name = image_url.rsplit('/', 1)[-1]
                file_path = os.path.join(parent_dir, file_name)
                if os.path.exists(file_path):
                    sync_db.set_illust_fetched(illust_id, i)
                    continue
                image_jobs.append(FetchImageJob(
                    file_path=file_path,
                    image_url=image_url,
                    illust_id=illust_id,
                    image_id=i,
                ))
        image_jobs.sort(key=lambda o: o.file_path)

        # download the images
        if image_jobs:
            print(f'Fetching {len(image_jobs)} images ...')
            process = CrawlerProcess()
            process.crawl(PixivImagesSpider, config, sync_db, image_jobs)
            process.start()


@pixiv_sync.command()
@click.option('-C', '--config-file', help='The YAML config file.',
              default='config.yml', required=True)
def sync(config_file):
    # first, check the login token
    config = load_config_file(config_file)
    sync_db = SyncDB(config['sync.db'])
    cookies = sync_db.get_cookies()
    if cookies and PIXIV_TOKEN_COOKIE_NAME in cookies:
        _ = get_logged_in_user_id(cookies)

    # next, do synchronization
    args_prefix = [sys.executable, os.path.abspath(__file__)]
    subprocess.check_call(args_prefix + ['sync-list', '-C', config_file])
    subprocess.check_call(args_prefix + ['sync-images', '-C', config_file])


if __name__ == '__main__':
    pixiv_sync()
