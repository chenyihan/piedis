# -*- coding: GBK -*-
'''
Created on 2015年7月9日

@author: yunyun
'''

DEFAULT_ENCODING = 'utf-8'

def decodeData(data):
    return data.decode(DEFAULT_ENCODING)

def encodeData(Str):
    return Str.encode(DEFAULT_ENCODING)