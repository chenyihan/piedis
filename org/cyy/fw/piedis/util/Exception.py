# -*- coding: GBK -*-
'''
Created on 2015��7��8��

@author: yunyun
'''
class PiedisException(Exception):
    def __init__(self, message, cause):
        self.message = message
        self.__cause__ = cause
    
    def __str__(self):
        return repr(self.message)

class ProtocolException(PiedisException):
    def __init__(self, message):
        self.message = message
    
    def __str__(self):
        return repr(self.message)
