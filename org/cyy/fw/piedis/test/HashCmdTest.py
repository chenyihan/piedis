# -*- coding: GBK -*-
'''
Created on 2015年7月20日

@author: yunyun
'''
from org.cyy.fw.piedis.test import BaseCmdTest
from org.cyy.fw.piedis.Command import KeyValuePair, ScanParams
class HashCmdTest(BaseCmdTest):
    def test_hset(self):
        self.client.flushAll()
        
        resp = self.client.hSet('key1', 'field1', 'value1')
        self.assertEquals(resp, True)
        
        resp = self.client.hSet('key1', 'field1', 'value1')
        self.assertEquals(resp, False)
        
        resp = self.client.hSet('key1', 'field2', 'value2')
        self.assertEquals(resp, True)
    
    def test_hsetNX(self):
        self.client.flushAll()
        
        resp = self.client.hSetNX('key1', 'field1', 'value1')
        self.assertEquals(resp, True)
        
        resp = self.client.hSetNX('key1', 'field1', 'value2')
        self.assertEquals(resp, False)
    
    def test_hget(self):
        self.client.flushAll()
        
        resp = self.client.hGet('hkey1', 'field1')
        self.assertEquals(resp, 'null')
        
        self.client.hSet('hkey1', 'field1', 'value1')
        
        resp = self.client.hGet('hkey1', 'field1')
        self.assertEquals(resp, 'value1')
    
    def test_hdel(self):
        self.client.flushAll()
        
        self.client.hSet('hkey1', 'field1', 'value1')
        
        resp = self.client.hGet('hkey1', 'field1')
        self.assertEquals(resp, 'value1')
        
        resp = self.client.hDel('hkey1', 'field1')
        self.assertEquals(resp, 1)
        
        resp = self.client.hGet('hkey1', 'field1')
        self.assertEquals(resp, 'null')
        
        resp = self.client.hDel('hkey1', 'field1')
        self.assertEquals(resp, 0)
        
    def test_hexist(self):
        self.client.flushAll()
        
        resp = self.client.hExist('hkey1', 'field1');
        self.assertEquals(resp, False)
        
        self.client.hSet('hkey1', 'field1', 'value1')
        resp = self.client.hExist('hkey1', 'field1');
        self.assertEquals(resp, True)
        
    def test_hgetall(self):
        self.client.flushAll()
        
        self.client.hSet('hkey1', 'field1', 'value1')
        self.client.hSet('hkey1', 'field2', 'value2')
        self.client.hSet('hkey1', 'field3', 'value3')
        
        resp = self.client.hGetAll('hkey1')
        self.assertTrue(isinstance(resp, dict))
        print(resp)
    
    def test_hincrby(self):
        self.client.flushAll()
        
        resp = self.client.hIncrBy('hkey1', 'hfield1', 200)
        self.assertEquals(resp, 200)
        
        self.client.hSet('hkey1', 'hfield1', 2)
        resp = self.client.hIncrBy('hkey1', 'hfield1', 200)
        self.assertEquals(resp, 202)
        
    def test_hincrByFloat(self):
        self.client.flushAll()
        
        resp = self.client.hIncrByFloat('hkey1', 'hfield1', 200.01)
        self.assertEquals(resp, 200.01)
        
        self.client.hSet('hkey1', 'hfield1', 2.2)
        resp = self.client.hIncrByFloat('hkey1', 'hfield1', 200.01)
        self.assertEquals(resp, 202.21)
        
    def test_hkeys(self):
        self.client.flushAll()
        
        resp = self.client.hKeys('hkey1')
        self.assertEquals(len(resp), 0)
        
        self.client.hSet('hkey1', 'field1', 'value1')
        self.client.hSet('hkey1', 'field2', 'value2')
        self.client.hSet('hkey1', 'field3', 'value3')
        
        resp = self.client.hKeys('hkey1')
        self.assertTrue(isinstance(resp, tuple))
        self.assertEquals(len(resp), 3)
        print(resp)
    
    def test_hvals(self):
        self.client.flushAll()
        
        resp = self.client.hVals('hkey1')
        self.assertEquals(len(resp), 0)
        
        self.client.hSet('hkey1', 'field1', 'value1')
        self.client.hSet('hkey1', 'field2', 'value2')
        self.client.hSet('hkey1', 'field3', 'value3')
        
        resp = self.client.hVals('hkey1')
        self.assertTrue(isinstance(resp, tuple))
        self.assertEquals(len(resp), 3)
    
    def test_hlen(self):
        self.client.flushAll()
        
        resp = self.client.hLen('hkey1')
        self.assertEquals(resp, 0)
        
        self.client.hSet('hkey1', 'field1', 'value1')
        self.client.hSet('hkey1', 'field2', 'value2')
        self.client.hSet('hkey1', 'field3', 'value3')
        
        resp = self.client.hLen('hkey1')
        self.assertEquals(resp, 3)
    
    def test_hmset(self):
        self.client.flushAll()
        
        resp = self.client.hMSet('hkey1', KeyValuePair('hfield1', 'hvalue1'), KeyValuePair('hfield2', 'hvalue2'), KeyValuePair('hfield3', 'hvalue3'))
        self.assertEquals(resp, 'OK')
        
        resp = self.client.hGetAll('hkey1')
        self.assertTrue(isinstance(resp, dict))
        print(resp)
    
    def test_hmget(self):
        self.client.flushAll()
        
        self.client.hMSet('hkey1', KeyValuePair('hfield1', 'hvalue1'), KeyValuePair('hfield2', 'hvalue2'), KeyValuePair('hfield3', 'hvalue3'))
        resp = self.client.hMGet('hkey1', 'hfield1', 'hfield3', 'hfield3', 'hfield4')
        self.assertTrue(isinstance(resp, tuple))
        print(resp)
        
    def test_hscan(self):
        self.client.flushAll()
        
        self.client.hSet('key1', 'field1', 'value1');
        self.client.hSet('key1', 'field2', 'value2');
        self.client.hSet('key1', 'field3', 'value3');
        self.client.hSet('key1', 'field4', 'value4');
        self.client.hSet('key1', 'field5', 'value5');
        self.client.hSet('key1', 'field6', 'value6');
        self.client.hSet('key1', 'field7', 'value7');
        self.client.hSet('key1', 'field8', 'value8');
        self.client.hSet('key1', 'field9', 'value9');
        self.client.hSet('key1', 'field10', 'value10');
        self.client.hSet('key1', 'field11', 'value11');
        self.client.hSet('key1', 'field12', 'value12');
        self.client.hSet('key1', 'field13', 'value13');
        self.client.hSet('key1', 'field14', 'value14');
        self.client.hSet('key1', 'field15', 'value15');
        self.client.hSet('key1', 'field16', 'value16');
        self.client.hSet('key1', 'field17', 'value17');
        self.client.hSet('key1', 'field18', 'value18');
        self.client.hSet('key1', 'field19', 'value19');
        self.client.hSet('key1', 'field20', 'value20');
        self.client.hSet('key1', 'field21', 'value21');
        self.client.hSet('key1', 'field22', 'value22');
        self.client.hSet('key1', 'field23', 'value23');
        self.client.hSet('key1', 'field24', 'value24');
        self.client.hSet('key1', 'field25', 'value25');
        self.client.hSet('key1', 'field26', 'value26');
        self.client.hSet('key1', 'field27', 'value27');
        self.client.hSet('key1', 'field28', 'value28');
        self.client.hSet('key1', 'field29', 'value29');
        self.client.hSet('key1', 'field30', 'value30');
        
        cursor = 0
        count = 5
        
        resp = self.client.hScan('key1', cursor, ScanParams().count(count))
        cursor = resp.getCursor()
        result = resp.getResults()
        print(cursor)
        print(result)
        
        resp = self.client.hScan('key1', cursor, ScanParams().count(count))
        cursor = resp.getCursor()
        result = resp.getResults()
        print(cursor)
        print(result)
        
        resp = self.client.hScan('key1', cursor, ScanParams().count(count))
        cursor = resp.getCursor()
        result = resp.getResults()
        print(cursor)
        print(result)
        
        
