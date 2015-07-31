# -*- coding: GBK -*-
'''
Created on 2015年7月29日

@author: yunyun
'''
from org.cyy.fw.piedis.test import BaseCmdTest
class ScriptCmdTest(BaseCmdTest):
    def testScript(self):
        self.client.flushAll()
        
        resp = self.client.eval('return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}', 2, 'key1', 'key2', 'first', 'second')
        print(resp)
    
    def testScript1(self):
        self.client.flushAll()
        
        lua_file = 'str_cmd.lua'
        
        with open(lua_file, mode='r', encoding='UTF-8') as lua_stream:
            script = lua_stream.read()
            resp = self.client.eval(script, 1, 'key1', 'value1')
            print(resp)
            resp = self.client.get('key1')
            print(resp)
            
    def testScript2(self):
        self.client.flushAll()
        
        key = 'key'
        for i in range(0, 97):
            value = 'value' + str(i)
            self.client.lPush(key, value)
            
        lua_file = 'list_lru_cmd.lua'
        maxLen = 100
        with open(lua_file, mode='r', encoding='UTF-8') as lua_stream:
            script = lua_stream.read()
            self.client.eval(script, 1, 'key', 'value98', maxLen)
            self.client.eval(script, 1, 'key', 'value99', maxLen)
            self.client.eval(script, 1, 'key', 'value100', maxLen)
            self.client.eval(script, 1, 'key', 'value101', maxLen)
            self.client.eval(script, 1, 'key', 'value102', maxLen)
        
        resp = self.client.lLen(key)
        self.assertEquals(resp, 100) 
    
    def testScript3(self):
        self.client.flushAll()
        self.client.scriptFlush()
        lua_file = 'list_lru_cmd.lua'
        maxLen = 100
        with open(lua_file, mode='r', encoding='UTF-8') as lua_stream:
            script = lua_stream.read()
            sha = self.client.scriptLoad(script)
            self.client.evalSha(sha, 1, 'key', 'value98', maxLen)
            resp = self.client.scriptExist(sha)
            print(resp)
    
    def testScript4(self):
        self.client.flushAll()
        self.client.scriptFlush()
        lua_file = 'list_lru_clear.lua'
        key1 = 'key1'
        for i in range(1, 105):
            value = 'value1' + str(i)
            self.client.lPush(key1, value)
        key2 = 'key2'
        for i in range(1, 105):
            value = 'value2' + str(i)
            self.client.lPush(key2, value)
        with open(lua_file, mode='r', encoding='UTF-8') as lua_stream:
            script = lua_stream.read()
            resp = self.client.eval(script, 0)
            print(resp)
        
        resp = self.client.lLen(key1)
        self.assertEquals(resp, 100) 
        
        resp = self.client.lLen(key2)
        self.assertEquals(resp, 100) 
        
