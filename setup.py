# -*- coding: GBK -*-
'''
Created on 2015年7月24日

@author: yunyun
'''
from distutils.core import setup
from setuptools import find_packages
NAME = 'piedis'
VERSION = '1.0.0'
DESCRIPTION = 'Redis client for python'
AUTHOR = 'chen yun yun'
AUTHOR_MAIL = '348800349@qq.com'
setup(name=NAME, version=VERSION, describe=DESCRIPTION, author=AUTHOR, author_mail=AUTHOR_MAIL, packages=find_packages())
