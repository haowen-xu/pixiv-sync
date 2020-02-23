"""Pixiv Sync Tool"""
import ast
import codecs
import os
import re
import sys
from setuptools import setup


_version_re = re.compile(r'__version__\s+=\s+(.*)')
_source_dir = os.path.split(os.path.abspath(__file__))[0]

if sys.version_info[0] == 2:
    def read_file(path):
        with open(path, 'rb') as f:
            return f.read()
else:
    def read_file(path):
        with codecs.open(path, 'rb', 'utf-8') as f:
            return f.read()

version = str(ast.literal_eval(_version_re.search(
    read_file(os.path.join(_source_dir, 'PixivSync.py'))).group(1)))

requirements_list = list(filter(
    lambda v: v and not v.startswith('#'),
    (s.strip() for s in read_file(
        os.path.join(_source_dir, 'requirements.txt')).split('\n'))
))
dependency_links = [s for s in requirements_list if s.startswith('git+')]
install_requires = [s for s in requirements_list if not s.startswith('git+')]


setup(
    name='pixiv-sync',
    version=version,
    url='https://github.com/haowen-xu/pixiv-sync/',
    license='MIT',
    author='Haowen Xu',
    author_email='haowen.xu@outlook.com',
    py_modules=['PixivSync'],
    platforms='any',
    setup_requires=['setuptools'],
    install_requires=install_requires,
    dependency_links=dependency_links,
    entry_points='''
    [console_scripts]
        PixivSync=PixivSync:pixiv_sync
    '''
)
