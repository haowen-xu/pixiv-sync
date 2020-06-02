#!/usr/bin/env python

import json
import os
import codecs
import re
import shutil
import sys
import threading
import traceback
from dataclasses import dataclass
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import *

import click
import yaml
from pprint import pprint
from pixivpy3 import AppPixivAPI

__version__ = '0.0.2'


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

            parent_dir = os.path.split(self.path)[0]
            if not os.path.isdir(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)

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

    def get_token(self, default=None):
        return self.get('token', default)

    def set_token(self, val: Dict[str, str]):
        self['token'] = val

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

    def set_illust_fetched(self, illust_id: str, image_id: int, fetched: bool = True):
        with self.lock:
            self['illusts'][illust_id]['images'][image_id]['fetched'] = fetched

    def get_user(self, user_id: str, default=None):
        return self._get_dict('users', user_id, default)

    def update_user(self, user_id: str, val: Dict[str, Any]):
        self._update_dict('users', user_id, val)


def is_set_intersect(a, b):
    if not isinstance(b, set):
        b = set(b)
    return any(i in b for i in a)


def is_illust_excluded(config, illust):
    # gather illust values
    info = {
        'authors': [illust[k] for k in ('author_id', 'author_name')
                    if k in illust],
        'tags': []
    }
    for tag in illust.get('tags', []):
        for k in ('name', 'translation'):
            if k in tag:
                info['tags'].append(tag[k])

    # test against rules
    includes = config.get('includes', {})
    includes = {k: includes[k] for k in info if k in includes}
    excludes = config.get('excludes', {})
    excludes = {k: excludes[k] for k in info if k in excludes}

    if includes:
        if not any(is_set_intersect(info[k], includes[k]) for k in includes):
            return True

    if excludes:
        if any(is_set_intersect(info[k], excludes[k]) for k in excludes):
            return True

    # default action
    return False


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


@click.group()
def pixiv_sync():
    """Pixiv illustrations sync tool."""


def make_api_client(sync_db: SyncDB) -> AppPixivAPI:
    api = AppPixivAPI()
    auth = sync_db.get_token()
    keys = ('access_token', 'device_token', 'refresh_token', 'user')
    if auth and all(k in auth for k in keys):
        api.access_token = auth['access_token']
        api.refresh_token = auth['refresh_token']
        api.user_id = auth['user']['id']
    return api


AUTHOR_ID_PATTERNS = [
    re.compile(r'^(\d+)$'),
    re.compile(r'^https?://www\.pixiv\.net/users/(\d+)(?:/.*)?')
]
ILLUST_ID_PATTERNS = [
    re.compile(r'/artworks/(\d+)(?:/.*)?$'),
]


def extract_illust_data(illust) -> Dict[str, Any]:
    def filter_dict(d):
        return {k: v for k, v in d.items() if v}

    def get_tags(illust_data):
        tags = []
        for t in illust_data.get('tags', []):
            t_name = t.get('name')
            if not t_name:
                continue
            t_translation = t.get('translated_name')
            tags.append(filter_dict({
                'name': t_name,
                'translation': t_translation
            }))
        return tags

    images = []

    # parse single page illust
    p_data = illust['meta_single_page']
    if p_data:
        images.append({
            'url': p_data['original_image_url'],
        })
    else:
        for p_data in illust['meta_pages']:
            images.append({
                'url': p_data['image_urls']['original'],
            })

    r = {
        'id': str(illust['id']),
        'title': illust['title'],
        'create_time': illust['create_date'],
        'author_id': str(illust['user']['id']),
        'author_name': illust['user']['name'],
        'tags': get_tags(illust),
        'width': illust['width'],
        'height': illust['height'],
        'images': images,
    }
    if any(not v for v in r):
        raise ValueError(f'Malformed response: {r}')
    return r


def update_list(sync_db: SyncDB, config: Dict[str, Any],
                max_bookmark_id: Optional[str] = None):
    """Pull the new illustrations that should be downloaded."""
    def store_illust(illust, counter):
        illust_id = str(illust['id'])
        if not sync_db.get_illust(illust_id):
            item = extract_illust_data(illust)
            item['_deleted'] = is_illust_excluded(config, item)
            if item:
                sync_db.update_illust(illust_id, item)
                counter += 1
        return counter

    api = make_api_client(sync_db)

    # get new illustrations list from interested authors
    for author_id_or_url in config.get('authors', []):
        author_id = None
        for pattern in AUTHOR_ID_PATTERNS:
            m = pattern.match(author_id_or_url)
            if m:
                author_id = m.group(1)
                break
        if author_id is None:
            raise ValueError(f'No author ID can be recognized from: '
                             f'{author_id_or_url}')

        print(f'> Pull from: {author_id_or_url}')
        offset = 0
        new_counter = 0
        try:
            while True:
                r = api.user_illusts(author_id, offset=offset)
                if 'error' in r:
                    raise Exception(r['error']['message'] or r['error']['user_message'])
                illusts = r['illusts']
                if not illusts:
                    break
                for illust in illusts:
                    new_counter = store_illust(illust, new_counter)
                offset += len(illusts)

        except Exception:
            print(''.join(traceback.format_exception(*sys.exc_info())) +
                  f'Failed to call `api.user_illusts`: user_id={author_id}, '
                  f'offset={offset}.')

        if new_counter > 0:
            print(f'Discovered {new_counter} new illusts.')

    # get new illustrations from user's bookmarks
    the_max_bookmark_id = max_bookmark_id
    if api.user_id:
        for fav in config.get('favourites', []):
            if fav not in ('public', 'private'):
                raise ValueError(f'Unknown favourite type: {fav}')

            max_bookmark_id = the_max_bookmark_id
            new_counter = 0

            while True:
                print(f'> Pull from bookmark: {fav} (max_bookmark_id={max_bookmark_id})')
                r = api.user_bookmarks_illust(
                    api.user_id, restrict=fav, max_bookmark_id=max_bookmark_id)
                if 'error' in r:
                    raise Exception(r['error']['message'] or r['error']['user_message'])

                # parse the illustrations
                illusts = r['illusts']
                if not illusts:
                    break

                old_new_counter = new_counter
                for illust in illusts:
                    new_counter = store_illust(illust, new_counter)

                # if no new illusts on this page, stop pulling
                if old_new_counter == new_counter:
                    break
                else:
                    print(f'Discovered {new_counter - old_new_counter} new illusts.')

                # parse the next bookmark
                next_url = r['next_url']
                if next_url:
                    m = re.match(r'.*[?&]max_bookmark_id=(\d+)(?:&|$)', next_url)
                    if m:
                        max_bookmark_id = m.group(1)
                    else:
                        break
    else:
        print('! User not logged in, bookmarks disabled.')

    # update "_deleted"
    for illust_id in sync_db.get_illust_ids():
        illust = sync_db.get_illust(illust_id, {})
        sync_db.update_illust(
            illust_id,
            {'_deleted': is_illust_excluded(config, illust)}
        )


@dataclass
class FetchImageJob(object):
    file_path: str
    image_url: str
    illust_id: str
    image_id: int


def fetch_images(sync_db: SyncDB, download_dir: str, n_workers: int):
    api = make_api_client(sync_db)

    # get the jobs of fetch images
    image_jobs: List[FetchImageJob] = []
    for illust_id in sync_db.get_illust_ids():
        illust = sync_db.get_illust(illust_id, {})
        if illust.get('_deleted', False):
            continue

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
            image_jobs.append(FetchImageJob(
                file_path=file_path,
                image_url=image_url,
                illust_id=illust_id,
                image_id=i,
            ))
    image_jobs.sort(key=lambda o: o.file_path)
    f_lock = threading.RLock()
    counter = [len(image_jobs)]

    def f_download(job: FetchImageJob):
        parent_dir, name = os.path.split(job.file_path)
        try:
            os.makedirs(parent_dir, exist_ok=True)
            api.download(
                url=job.image_url,
                path=parent_dir,
                name=name,
                replace=True,
            )
            with f_lock:
                counter[0] -= 1
                sync_db.set_illust_fetched(job.illust_id, job.image_id)
                print(f'[{counter[0]}/{len(image_jobs)}] done: {job.image_url}')
        except:
            try:
                os.remove(job.file_path)
            except Exception:
                pass
            print(''.join(traceback.format_exception(*sys.exc_info())) +
                  f'Failed to download: {job.image_url}')

    if image_jobs:
        print(f'> Fetching {len(image_jobs)} images ...')
        pool = ThreadPool(processes=n_workers)
        pool.map(f_download, image_jobs)
        pool.close()
        pool.join()


def _remove_illust(download_dir, sync_db, illust_ids):
    for illust_id in illust_ids:
        illust = sync_db.get_illust(illust_id)
        if illust:
            author_name = illust['author_name']
            parent_dir = os.path.join(download_dir, author_name)
            remove_parent_dir = False

            images = illust.get('images', [])
            if len(images) > 1:
                parent_dir = os.path.join(parent_dir, illust_id)
                remove_parent_dir = True

            for i, image in enumerate(images):
                if not image.get('fetched', False):
                    continue
                image_url = image['url']
                file_name = image_url.rsplit('/', 1)[-1]
                file_path = os.path.join(parent_dir, file_name)
                is_removed = not os.path.exists(file_path)

                if not is_removed:
                    try:
                        os.remove(file_path)
                        is_removed = True
                        print(f'Removed: {file_path}')
                    except Exception:
                        print(f'Failed to remove: {file_path}')
                        print(''.join(traceback.format_exception(*sys.exc_info())))

                if is_removed:
                    sync_db.set_illust_fetched(illust_id, i, False)

            if remove_parent_dir and os.path.exists(parent_dir):
                try:
                    shutil.rmtree(parent_dir)
                except Exception:
                    print(f'Failed to rmtree: {parent_dir}')
                    print(''.join(traceback.format_exception(*sys.exc_info())))

            sync_db.update_illust(illust_id, {'_deleted': True})


def _count_db(sync_db, download_dir):
    counts = {
        'illust': [],
        'deleted_illust': [],
        'images': [],
        'deleted_images': [],
        'not_exist_images': [],
        'not_deleted_images': [],
    }
    for illust_id in sync_db.get_illust_ids():
        illust = sync_db.get_illust(illust_id, {})
        deleted = illust.get('_deleted')
        counts['deleted_illust' if deleted else 'illust'].append(illust_id)

        author_name = illust['author_name']
        parent_dir = os.path.join(download_dir, author_name)
        images = illust.get('images', [])
        if len(images) > 1:
            parent_dir = os.path.join(parent_dir, illust_id)

        for i, image in enumerate(images):
            image_url = image['url']
            file_name = image_url.rsplit('/', 1)[-1]
            file_path = os.path.join(parent_dir, file_name)

            if os.path.exists(file_path):
                counts['not_deleted_images' if deleted else 'images'].append(file_path)
            else:
                counts['deleted_images' if deleted else 'not_exist_images'].append(file_path)

    return counts


@pixiv_sync.command()
@click.option('-C', '--config-file', help='The YAML config file.',
              default='config.yml', required=True)
@click.argument('username', type=str, required=True)
@click.argument('password', type=str, required=True)
def login(config_file, username, password):
    """Login with username and password and obtain authentication token."""
    config = load_config_file(config_file)
    with SyncDB(config['sync.db']) as sync_db:
        api = AppPixivAPI()
        token = api.login(username, password)['response']
        pprint(token)
        sync_db.set_token(token)


@pixiv_sync.command()
@click.option('-C', '--config-file', help='The YAML config file.',
              default='config.yml', required=True)
@click.option('--list-only', is_flag=True, default=False)
@click.option('--fetch-only', is_flag=True, default=False)
@click.option('--max-bookmark-id', required=False, default=None)
def sync(config_file, list_only, fetch_only, max_bookmark_id):
    """Synchronize the illustrations."""
    config = load_config_file(config_file)
    download_dir = os.path.abspath(config['download.dir'])
    n_workers = config.get('download.workers', 8)

    with SyncDB(config['sync.db']) as sync_db:
        if not fetch_only:
            update_list(sync_db, config, max_bookmark_id=max_bookmark_id)
        if not list_only:
            print('')
            fetch_images(sync_db, download_dir, n_workers)


@pixiv_sync.command()
@click.option('-C', '--config-file', help='The YAML config file.',
              default='config.yml', required=True)
@click.argument('illust_ids', nargs=-1)
def remove(config_file, illust_ids):
    """Delete illusts."""
    config = load_config_file(config_file)
    sync_db = SyncDB(config['sync.db'])
    download_dir = os.path.abspath(config['download.dir'])

    with sync_db:
        _remove_illust(download_dir, sync_db, illust_ids)


@pixiv_sync.command()
@click.option('-C', '--config-file', help='The YAML config file.',
              default='config.yml', required=True)
@click.option('-S', '--simulate', is_flag=True, default=False)
@click.option('-I', '--show-info', is_flag=True, default=False)
def remove_excluded(config_file, simulate, show_info):
    """Delete excluded illusts."""
    config = load_config_file(config_file)
    sync_db = SyncDB(config['sync.db'])
    download_dir = os.path.abspath(config['download.dir'])
    delete_ids = []

    with sync_db:
        for illust_id in sync_db.get_illust_ids():
            illust = sync_db.get_illust(illust_id, {})
            if not illust.get('_deleted', False) and is_illust_excluded(config, illust):
                delete_ids.append(illust_id)
                if show_info:
                    title = f'Info for {illust_id}'
                    print(title + '\n' + '-' * len(title))
                    pprint(sync_db.get_illust(illust_id))
                    print('')

        print(f'Found {len(delete_ids)} illusts to remove.')
        if not simulate:
            _remove_illust(download_dir, sync_db, delete_ids)


@pixiv_sync.command()
@click.option('-C', '--config-file', help='The YAML config file.',
              default='config.yml', required=True)
def count(config_file):
    """Count downloaded illusts."""
    config = load_config_file(config_file)
    download_dir = os.path.abspath(config['download.dir'])
    with SyncDB(config['sync.db']) as sync_db:
        counts = _count_db(sync_db, download_dir)
        pprint({k: len(counts[k]) for k in counts})


if __name__ == '__main__':
    pixiv_sync()
