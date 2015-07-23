# -*- coding: GBK -*-
'''
Created on 2015年7月20日

@author: yunyun
'''
from org.cyy.fw.piedis.test import BaseCmdTest
class ListCmdTest(BaseCmdTest):
    def test_lpush(self):
        self.client.flushAll()
        
        resp = self.client.lPush('llkey1', 'llvalue1')
        self.assertEquals(resp, 1)
        
        self.client.flushAll()
        resp = self.client.lPush('llkey1', 'llvalue1', 'llvalue2')
        self.assertEquals(resp, 2)
    
    def test_lpushX(self):
        self.client.flushAll()
        
        resp = self.client.lPushX('llkey1', 'llvalue1')
        self.assertEquals(resp, 0)
        
        self.client.lPush('llkey1', 'llvalue1', 'llvalue2')
        resp = self.client.lPushX('llkey1', 'llvalue3')
        self.assertEquals(resp, 3)
        
    def test_lpop(self):
        self.client.flushAll()
        
        self.client.lPush('llkey1', 'llvalue1', 'llvalue2')
        
        resp = self.client.lPop('llkey1')
        self.assertEquals(resp, 'llvalue2')
        
        
        resp = self.client.lPop('llkey1')
        self.assertEquals(resp, 'llvalue1')
        
    def test_rpush(self):
        self.client.flushAll()
        
        resp = self.client.rPush('lrkey1', 'lrvalue1')
        self.assertEquals(resp, 1)
        
        self.client.flushAll()
        resp = self.client.rPush('lrkey1', 'lrvalue1', 'lrvalue2')
        self.assertEquals(resp, 2)
    
    def test_rpushX(self):
        self.client.flushAll()
        
        resp = self.client.rPushX('lrkey1', 'lrvalue1')
        self.assertEquals(resp, 0)
        
        self.client.rPush('lrkey1', 'lrvalue1', 'lrvalue2')
        resp = self.client.rPushX('lrkey1', 'llvalue3')
        self.assertEquals(resp, 3)
    
    def test_rpop(self):
        self.client.flushAll()
        
        self.client.rPush('lrkey1', 'lrvalue1', 'lrvalue2')
        
        resp = self.client.rPop('lrkey1')
        self.assertEquals(resp, 'lrvalue2')
        resp = self.client.rPop('lrkey1')
        self.assertEquals(resp, 'lrvalue1')
    
    def test_blpop(self):
        self.client.flushAll()
        
        resp = self.client.blPop(5, 'lrkey1', 'lrkey2')
        empty = ()
        self.assertEquals(resp, empty)
        self.client.rPush('lrkey1', 'lrvalue1', 'lrvalue2')
        resp = self.client.blPop(30, 'lrkey1')
        self.assertEquals(resp, ('lrkey1', 'lrvalue1'))
        
    def test_brpop(self):
        self.client.flushAll()
        
        resp = self.client.brPop(5, 'lrkey1', 'lrkey2')
        empty = ()
        self.assertEquals(resp, empty)
        
        self.client.rPush('lrkey1', 'lrvalue1', 'lrvalue2')
        resp = self.client.brPop(30, 'lrkey1', 'lrkey2');
        self.assertEquals(resp, ('lrkey1', 'lrvalue2'))
        
    def test_lrange(self):
        self.client.flushAll()
        
        self.client.lPush('llkey1', 'llvalue1', 'llvalue2', 'llvalue3')
        
        resp = self.client.lRange('llkey1', 0, -1)
        self.assertEquals(resp, ('llvalue3', 'llvalue2', 'llvalue1'))
        
        resp = self.client.lRange('llkey1', 0, 1)
        self.assertEquals(resp, ('llvalue3', 'llvalue2'))
        
        
        resp = self.client.lRange('llkey1', 0, 2)
        self.assertEquals(resp, ('llvalue3', 'llvalue2', 'llvalue1'))
        
        resp = self.client.lRange('llkey1', 0, 3)
        self.assertEquals(resp, ('llvalue3', 'llvalue2', 'llvalue1'))
        
    def test_rpoplpush(self): 
        self.client.flushAll()  
        self.client.lPush('llkey1', 'llvalue1', 'llvalue2', 'llvalue3')
        self.client.lPush('llkey2', 'llvalue21', 'llvalue22', 'llvalue23')
        
        resp = self.client.rPopLPush('llkey1', 'llkey2')
        self.assertEquals(resp, 'llvalue1')
        
        resp = self.client.lRange('llkey1', 0, -1)
        self.assertEquals(resp, ('llvalue3', 'llvalue2'))
        
        
        resp = self.client.lRange('llkey2', 0, -1)
        self.assertEquals(resp, ('llvalue1', 'llvalue23', 'llvalue22', 'llvalue21'))
        
    def test_brpoplpush(self):
        self.client.flushAll()  
        self.client.bRPopLPush('llkey1', 'llkey2', 5)
        
        self.client.lPush('llkey1', 'llvalue1', 'llvalue2', 'llvalue3')
        self.client.lPush('llkey2', 'llvalue21', 'llvalue22', 'llvalue23')
        
        resp = self.client.bRPopLPush('llkey1', 'llkey2', 5)
        self.assertEquals(resp, 'llvalue1')
    
    def test_lindex(self):
        self.client.flushAll()  
        self.client.lPush('llkey1', 'llvalue1', 'llvalue2', 'llvalue3')
        
        resp = self.client.lIndex('llkey1', 0)
        self.assertEquals(resp, 'llvalue3')
        
        resp = self.client.lIndex('llkey1', 2)
        self.assertEquals(resp, 'llvalue1')
        
        resp = self.client.lIndex('llkey1', 4)
        self.assertEquals(resp, 'null')
    
    def test_linsert(self):
        self.client.flushAll()  
        self.client.lPush('llkey1', 'llvalue1', 'llvalue2', 'llvalue3')
        
        resp = self.client.lInsert('llkey2', 'llvalue4', 'llvalue5', True)
        self.assertEquals(resp, 0)
        
        resp = self.client.lInsert('llkey1', 'llvalue4', 'llvalue5', True)
        self.assertEquals(resp, -1)
        
        resp = self.client.lInsert('llkey1', 'llvalue4', 'llvalue2', True)
        self.assertEquals(resp, 4)
        
        resp = self.client.lRange('llkey1', 0, -1)
        self.assertEquals(resp, ('llvalue3', 'llvalue4', 'llvalue2', 'llvalue1'))
        
        resp = self.client.lInsert('llkey1', 'llvalue5', 'llvalue2', False)
        self.assertEquals(resp, 5)
        
        resp = self.client.lRange('llkey1', 0, -1)
        self.assertEquals(resp, ('llvalue3', 'llvalue4', 'llvalue2', 'llvalue5', 'llvalue1'))
        
    def test_llen(self):
        self.client.flushAll()  
        
        resp = self.client.lLen('llkey1')
        self.assertEquals(resp, 0)
        
        self.client.lPush('llkey1', 'llvalue1', 'llvalue2', 'llvalue3')
        
        resp = self.client.lLen('llkey1')
        self.assertEquals(resp, 3)
    
    def test_lrem(self):
        self.client.flushAll()  
        
        self.client.lPush('llkey1', 'llvalue1', 'llvalue2', 'llvalue3')
        
        resp = self.client.lREM('llkey1', 'llvalue1', 0)
        self.assertEquals(resp, 1)
        
        resp = self.client.lRange('llkey1', 0, -1)
        self.assertEquals(resp, ('llvalue3', 'llvalue2'))
        
    def test_lset(self):
        self.client.flushAll()
        
        self.client.lPush('llkey1', 'llvalue1', 'llvalue2', 'llvalue3')
        resp = self.client.lSet('llkey1', 1, 'llvalue22')
        self.assertEquals(resp, 'OK')
        
        resp = self.client.lRange('llkey1', 0, -1)
        self.assertEquals(resp, ('llvalue3', 'llvalue22', 'llvalue1'))
        
        try:
            self.client.lSet('llkey1', 3, 'llvalue4')
        except Exception as e:
            print(e)
        
        resp = self.client.lRange('llkey1', 0, -1)
        self.assertEquals(resp, ('llvalue3', 'llvalue22', 'llvalue1'))
    
    def test_ltrim(self):
        self.client.flushAll()
        
        self.client.lPush('llkey1', 'llvalue1', 'llvalue2', 'llvalue3')
        
        resp = self.client.lTrim('llkey1', 1, 2)
        self.assertEquals(resp, 'OK')
        
        resp = self.client.lRange('llkey1', 0, -1)
        self.assertEquals(resp, ('llvalue2', 'llvalue1'))
        
