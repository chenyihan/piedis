# -*- coding: GBK -*-
'''
Created on 2015年7月24日

@author: yunyun
'''
import unittest
from org.cyy.fw.piedis.Server import ServerNode
from org.cyy.fw.piedis.ShardedClient import ShardedClient

class ShardCmdTest(unittest.TestCase):
    def setUp(self):
        node1 = ServerNode().setHost('192.168.1.105').setPort(6379)
        node2 = ServerNode().setHost('192.168.1.107').setPort(6379)
        self.client = ShardedClient([node1, node2])
        
    def tearDown(self):
        self.client.close()
    
    def test_expire(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            self.client.set(key, value)
            resp = self.client.expire(key, 100)
            self.assertEquals(resp, True)
        
    def test_expireAt(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            self.client.set(key, value)
            resp = self.client.expireAt(key, 2355292000)
            self.assertEquals(resp, True)
    
    def test_move(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            self.client.set(key, value)
            resp = self.client.move(key, 1)
            self.assertEquals(resp, True)
            
    def test_objectrefcount(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            self.client.set(key, value)
            resp = self.client.objectRefcount(key)
            self.assertEquals(resp, 1)
            
    def test_objectidletime(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            self.client.set(key, value)
            resp = self.client.objectIdletime(key)
            print('idle time:', resp)
            
    def test_objectencoding(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            self.client.set(key, value)
            resp = self.client.objectEncoding(key)
            print('encoding:', resp)
            
    def test_persist(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            self.client.set(key, value)
            resp = self.client.persist(key)
            self.assertEquals(resp, False)
            
    def test_pexpire(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            self.client.set(key, value)
            resp = self.client.pExpire(key, 10000)
            print(resp)
    
    def test_pexpireAt(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            self.client.set(key, value)
            resp = self.client.pExpireAt(key, 2355292000)
            print(resp)
            
    def test_ttl(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            self.client.set(key, value)
            self.client.expireAt(key, 2355292000)
            resp = self.client.ttl(key)
            print(resp)
    
    def test_pttl(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            self.client.set(key, value)
            self.client.pExpireAt(key, 2355292000000)
            resp = self.client.pTTL(key)
            print(resp)
            
    def test_sort(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            self.client.lPush(key, "25", "3", "5", "4", "55", "34", "15", "2")
            
            resp = self.client.sort(key, None)
            print(resp)
    
    def test_setandget(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            resp = self.client.set(key, value)
            self.assertEquals(resp, 'OK')
            resp = self.client.get(key)
            self.assertEquals(resp, value)
            
    def test_type(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            self.client.set(key, value)
            resp = self.client.type(key)
            print(resp)
            
    def test_del(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            self.client.set(key, value)
            resp = self.client.Del(key)
            self.assertEquals(resp, 1)
            
    def test_exist(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            value = 'value-' + str(i)
            self.client.set(key, value)
            resp = self.client.exists(key)
            self.assertEquals(resp, True)
            
    def test_append(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            resp = self.client.append(key, 111)
            self.assertEquals(resp, 3)
            
            resp = self.client.append(key, 222)
            self.assertEquals(resp, 6)
    
    def test_hsetandget(self):
        self.client.flushAll()
        
        count = 10
        for i in range(0, count):
            key = 'key-' + str(i)
            field = 'field-' + str(i)
            value = 'value-' + str(i)
            resp = self.client.hSet(key, field, value)
            self.assertEquals(resp, True)
            
            resp = self.client.hGet(key, field)
            self.assertEquals(resp, value)
            
