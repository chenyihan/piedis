# -*- coding: GBK -*-
'''
Created on 2015年7月10日

@author: yunyun
'''
from org.cyy.fw.piedis.test import BaseCmdTest
from org.cyy.fw.piedis import RedisKeyword
from org.cyy.fw.piedis.Command import KeyValuePair


class StringCmdTest(BaseCmdTest):
    
    def setUp(self):
        BaseCmdTest.setUp(self)
    def tearDown(self):
        BaseCmdTest.tearDown(self)
    def test_set(self):
        self.client.flushAll()
        
        resp = self.client.set("key1", "value1")
        self.assertEqual(resp, "OK")
       
        resp = self.client.set("key1", "value2");
        self.assertEqual(resp, "OK")
    
    def test_get(self):
        self.client.flushAll()
        
        self.client.set("key1", "value1")
        
        resp = self.client.get("key1")
        self.assertEqual(resp, "value1");
        
        resp = self.client.get("key2")
        self.assertEqual(resp, "null");
        
    def test_del(self):
        self.client.flushAll()
        
        resp = self.client.Del('key1')
        self.assertEqual(resp, 0)
        
        self.client.set("key1", "value1")
        
        resp = self.client.Del('key1')
        self.assertEqual(resp, 1)
        
        self.client.set("key1", "value1")
        self.client.set("key2", "value2")
        
        resp = self.client.Del('key1', 'key2')
        self.assertEqual(resp, 2)
        
    def test_keys(self):
        self.client.flushAll()
        
        resp = self.client.keys('*')
        print(resp)
        self.assertEqual(len(resp), 0)
        
        self.client.set("key1", "value1")
        self.client.set("key2", "value2")
        
        resp = self.client.keys('key*')
        print(resp)
        self.assertEqual(len(resp), 2)
        
    def test_exist(self):
        self.client.flushAll()
        
        resp = self.client.exists('key1')
        self.assertEquals(resp, False)
        
        self.client.set("key1", "value1")
        
        resp = self.client.exists('key1')
        self.assertEquals(resp, True)
        
    def test_append(self):
        self.client.flushAll()
        
        resp = self.client.append('key1', '111')
        self.assertEquals(resp, 3)
        
        resp = self.client.append('key1', '222')
        self.assertEquals(resp, 6)
        
    def test_bit(self):
        self.client.flushAll()
        
        resp = self.client.setBit('key1', 10086, 1)
        self.assertEquals(resp, 0)
        
        resp = self.client.getBit('key1', 10086)
        self.assertEquals(resp, 1)
        
        resp = self.client.getBit('key1', 100)
        self.assertEquals(resp, 0)
        
        resp = self.client.bitCount('key1', None, None)
        self.assertEquals(resp, 1)
        
        resp = self.client.bitCount('key1', 0, 100)
        self.assertEquals(resp, 0)
        
        resp = self.client.setBit('key1', 100, 1)
        self.assertEquals(resp, 0)
        
        resp = self.client.bitCount('key1', 0, 100)
        self.assertEquals(resp, 1)
        
        resp = self.client.bitCount('key1', None, None)
        self.assertEquals(resp, 2)
        
    def test_bitOP(self):
        self.client.flushAll()
        
        self.client.setBit('key1', 0, 1);
        self.client.setBit('key1', 3, 1);
        self.client.setBit('key2', 0, 1);
        self.client.setBit('key2', 1, 1);
        self.client.setBit('key2', 3, 1);
        
        resp = self.client.bitop(RedisKeyword.AND, 'key3', 'key1', 'key2')
        print(resp)
        
        resp = self.client.bitop(RedisKeyword.XOR, 'key4', 'key1', 'key2')
        print(resp)
        
        resp = self.client.bitop(RedisKeyword.OR, 'key5', 'key1', 'key2')
        print(resp)
        
        resp = self.client.bitop(RedisKeyword.NOT, 'key6', 'key1')
        print(resp)
    
    def test_incr(self):
        self.client.flushAll()
        
        resp = self.client.incr('key1')
        self.assertEquals(resp, 1)
        
        resp = self.client.get('key1')
        self.assertEquals(resp, '1')
        
        self.client.set('key2', 43)
        resp = self.client.incr('key2')
        self.assertEquals(resp, 44)
        
        
        resp = self.client.get('key2')
        self.assertEquals(resp, '44')
        
    def test_incrby(self):
        self.client.flushAll()
        
        resp = self.client.incrBy('key1', 2)
        self.assertEquals(resp, 2)
        
        resp = self.client.get('key1')
        self.assertEquals(resp, '2')
        
        self.client.set('key2', 43)
        resp = self.client.incrBy('key2', 3)
        self.assertEquals(resp, 46)
        
    def test_incrbyfloat(self):
        self.client.flushAll()
        
        resp = self.client.incrByFloat('key1', 2.1)
        self.assertEquals(resp, 2.1)
        
        self.client.set('key2', 43)
        resp = self.client.incrByFloat('key2', 3.444444)
        
        self.assertEquals(resp, 46.444444)
        
    def test_decr(self):
        self.client.flushAll()
        
        resp = self.client.decr('key1')
        self.assertEquals(resp, -1)
        
        self.client.set('key2', 43)
        resp = self.client.decr('key2')
        self.assertEquals(resp, 42)
    
    def test_decrby(self):
        self.client.flushAll()
        resp = self.client.decrBy('key1', 2)
        self.assertEquals(resp, -2)
        
        
        self.client.set('key2', 43)
        resp = self.client.decrBy('key2', 2)
        self.assertEquals(resp, 41)
        
    def test_getrange(self):
        self.client.flushAll()
        
        self.client.set('key1', 'hello, my friend')
        resp = self.client.getRange('key1', 0, 4)
        self.assertEquals(resp, 'hello')
        
        
        resp = self.client.getRange('key1', -1, -5)
        self.assertEquals(resp, '')
        
        
        resp = self.client.getRange('key1', -3, -1)
        self.assertEquals(resp, 'end')
        
        resp = self.client.getRange('key1', 0, -1)
        self.assertEquals(resp, 'hello, my friend')
        
        resp = self.client.getRange('key1', 0, 1008611)
        self.assertEquals(resp, 'hello, my friend')
        
    def test_getset(self):
        self.client.flushAll()
        
        resp = self.client.getSet('key1', 'value1')
        self.assertEquals(resp, 'null')
        
        resp = self.client.get('key1')
        self.assertEquals(resp, 'value1')
        
        resp = self.client.getSet('key1', 'value2')
        self.assertEquals(resp, 'value1')
        
        resp = self.client.get('key1')
        self.assertEquals(resp, 'value2')
        
    def test_mgetAndset(self):
        self.client.flushAll()
        
        resp = self.client.mSet(KeyValuePair('key1', 'value1'), KeyValuePair('key2', 'value2'), KeyValuePair('key3', 'value3'))
        self.assertEquals(resp, 'OK')
        
        resp = self.client.get('key1')
        self.assertEquals(resp, 'value1')
        resp = self.client.get('key2')
        self.assertEquals(resp, 'value2')
        resp = self.client.get('key3')
        self.assertEquals(resp, 'value3')
        
        resp = self.client.mGet('key1', 'key2', 'key3', 'key4')
        print(resp)
        
        self.client.set('key1', 'value1')
        resp = self.client.mGet('key1')
        print(resp)
        
        resp = self.client.mSetNX(KeyValuePair('key1', 'value1'), KeyValuePair('key4', 'value4'))
        self.assertTrue(not resp)
        
        resp = self.client.mSetNX(KeyValuePair('key5', 'value5'), KeyValuePair('key4', 'value4'))
        self.assertTrue(resp)
        
        resp = self.client.setNX('key5', 'value5')
        self.assertTrue(not resp)
        
        resp = self.client.setNX('key6', 'value6')
        self.assertTrue(resp)
        
    def test_setEX(self):
        self.client.flushAll()
        
        resp = self.client.setEX('key1', 100, 'value1')
        self.assertEquals(resp, 'OK')
        
        resp = self.client.get('key1')
        self.assertEquals(resp, 'value1')
        
        resp = self.client.ttl('key1')
        print(resp)
    
    def test_psetEX(self):
        self.client.flushAll()
        
        resp = self.client.pSetEX('key1', 100000, 'value1')
        self.assertEquals(resp, 'OK')
        
        resp = self.client.get('key1')
        self.assertEquals(resp, 'value1')
        
        resp = self.client.pTTL('key1')
        print(resp)
        
    def test_setrange(self):
        self.client.flushAll()
        
        self.client.set('key1', 'hello world')
        
        resp = self.client.setRange('key1', 6, 'Redis')
        self.assertEquals(resp, 11)
        
        resp = self.client.get('key1')
        self.assertEquals(resp, 'hello Redis')
        
    def test_strlen(self):
        self.client.flushAll()
        
        self.client.set('key1', 'Hello world')
        
        resp = self.client.strLen('key1')
        self.assertEquals(resp, 11)
