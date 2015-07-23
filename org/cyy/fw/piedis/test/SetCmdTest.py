# -*- coding: GBK -*-
'''
Created on 2015年7月21日

@author: yunyun
'''
from org.cyy.fw.piedis.test import BaseCmdTest
from org.cyy.fw.piedis.Command import ScanParams
class SetCmdTest(BaseCmdTest):
    def test_sadd(self):
        self.client.flushAll()
        
        resp = self.client.sAdd('skey1', 'svalue1')
        self.assertEquals(resp, 1)
        
        resp = self.client.sAdd('skey1', 'svalue1')
        self.assertEquals(resp, 0)
        
        resp = self.client.sAdd('skey1', 'svalue2', 'svalue3')
        self.assertEquals(resp, 2)
        
    def test_spop(self):
        self.client.flushAll()
        
        self.client.sAdd('skey1', 'svalue1', 'svalue2', 'svalue3')
        
        resp = self.client.sPop('skey1')
        self.assertNotEquals(resp, 'null')
        
        resp = self.client.sPop('skey1')
        self.assertNotEquals(resp, 'null')
        
        resp = self.client.sPop('skey1')
        self.assertNotEquals(resp, 'null')
        
        resp = self.client.sPop('skey1')
        self.assertEquals(resp, 'null')
    
    def test_srandmember(self):
        self.client.flushAll()
        
        self.client.sAdd('skey1', 'svalue1', 'svalue2', 'svalue3')
        
        resp = self.client.sRandMember('skey1', 1)
        self.assertEquals(len(resp), 1)
        
        
        resp = self.client.sRandMember('skey1', 2)
        self.assertEquals(len(resp), 2)
        
        resp = self.client.sRandMember('skey1', 3)
        self.assertEquals(len(resp), 3)
        
        resp = self.client.sRandMember('skey1', 4)
        self.assertEquals(len(resp), 3)
        
        resp = self.client.sRandMember('skey1', -2)
        self.assertEquals(len(resp), 2)
        
        resp = self.client.sRandMember('skey1', -3)
        self.assertEquals(len(resp), 3)
        
        resp = self.client.sRandMember('skey1', -4)
        self.assertEquals(len(resp), 4)
    
    def test_scard(self):
        self.client.flushAll()
        
        resp = self.client.sCard('skey1')
        self.assertEquals(resp, 0)
        
        self.client.sAdd('skey1', 'svalue1', 'svalue2', 'svalue3')
        
        resp = self.client.sCard('skey1')
        self.assertEquals(resp, 3)
    
    def test_sdiff(self):
        self.client.flushAll()
        
        self.client.sAdd('skey1', 'svalue1', 'svalue2', 'svalue3')
        self.client.sAdd('skey2', 'svalue4', 'svalue2', 'svalue3')
        
        resp = self.client.sDiff('skey1', 'skey2')
        print(resp)
    
    def test_sdiffstore(self):
        self.client.flushAll()
        
        self.client.sAdd('skey1', 'svalue1', 'svalue2', 'svalue3')
        self.client.sAdd('skey2', 'svalue4', 'svalue2', 'svalue3')
        
        resp = self.client.sDiffStore('skey3', 'skey1', 'skey2')
        self.assertEquals(resp, 1)
        
        resp = self.client.sMembers('skey3')
        print(resp)
    
    def test_sinter(self):
        self.client.flushAll()
        
        self.client.sAdd('skey1', 'svalue1', 'svalue2', 'svalue3')
        self.client.sAdd('skey2', 'svalue4', 'svalue2', 'svalue3')
        
        resp = self.client.sInter('skey1', 'skey2')
        print(resp)
    
    def test_sinterstore(self):
        self.client.flushAll()
        
        self.client.sAdd('skey1', 'svalue1', 'svalue2', 'svalue3')
        self.client.sAdd('skey2', 'svalue4', 'svalue2', 'svalue3')
        
        resp = self.client.sInterStore('skey3', 'skey1', 'skey2')
        self.assertEquals(resp, 2)
        
        resp = self.client.sMembers('skey3')
        print(resp)
        
    def test_sismember(self):
        self.client.flushAll()
        
        self.client.sAdd('skey1', 'svalue1', 'svalue2', 'svalue3')
        
        resp = self.client.sisMember('skey1', 'skey2')
        self.assertTrue(not resp)
        
        resp = self.client.sisMember('skey1', 'svalue2')
        self.assertTrue(resp)
    
    def test_smove(self):
        self.client.flushAll()
        
        resp = self.client.sMove('skey1', 'skey2', 'svalue1')
        self.assertTrue(not resp)
        
        self.client.sAdd('skey1', 'svalue1', 'svalue2', 'svalue3')
        
        resp = self.client.sMembers('skey2')
        self.assertEquals(len(resp), 0)
        
        resp = self.client.sMove('skey1', 'skey2', 'svalue1')
        self.assertTrue(resp)
        
        resp = self.client.sMembers('skey2')
        self.assertEquals(len(resp), 1)
        
        resp = self.client.sMembers('skey1')
        self.assertEquals(len(resp), 2)
    
    def test_srem(self):
        self.client.flushAll()
        self.client.sAdd('skey1', 'svalue1', 'svalue2', 'svalue3')
        
        resp = self.client.sRem('skey1', 'svalue4', 'svalue2')
        self.assertEquals(resp, 1)
        
        resp = self.client.sMembers('skey1')
        self.assertEquals(len(resp), 2)
    
    def test_suion(self):
        self.client.flushAll()
        
        self.client.sAdd('skey1', 'svalue1', 'svalue2', 'svalue3')
        self.client.sAdd('skey2', 'svalue4', 'svalue2', 'svalue3')
        
        resp = self.client.sUnion('skey1', 'skey2')
        print(resp)
    
    def test_suionstore(self):
        self.client.flushAll()
        
        self.client.sAdd('skey1', 'svalue1', 'svalue2', 'svalue3')
        self.client.sAdd('skey2', 'svalue4', 'svalue2', 'svalue3')
        
        resp = self.client.sUnionStore('skey3', 'skey1', 'skey2')
        self.assertEquals(resp, 4)
        
        resp = self.client.sMembers('skey3')
        self.assertEquals(len(resp), 4)
    
    def test_sscan(self):
        self.client.flushAll()
        
        self.client.sAdd('key1', 'value1')
        self.client.sAdd('key1', 'value2')
        self.client.sAdd('key1', 'value3')
        self.client.sAdd('key1', 'value4')
        self.client.sAdd('key1', 'value5')
        self.client.sAdd('key1', 'value6')
        self.client.sAdd('key1', 'value7')
        self.client.sAdd('key1', 'value8')
        self.client.sAdd('key1', 'value9')
        self.client.sAdd('key1', 'value10')
        self.client.sAdd('key1', 'value11')
        self.client.sAdd('key1', 'value12')
        self.client.sAdd('key1', 'value13')
        self.client.sAdd('key1', 'value14')
        self.client.sAdd('key1', 'value15')
        self.client.sAdd('key1', 'value16')
        self.client.sAdd('key1', 'value17')
        self.client.sAdd('key1', 'value18')
        self.client.sAdd('key1', 'value19')
        self.client.sAdd('key1', 'value20')
        
        count = 5
        cursor = 0
        resp = self.client.sScan('key1', cursor, ScanParams().count(count))
        cursor = resp.getCursor()
        result = resp.getResults()
        print(result)
        
        resp = self.client.sScan('key1', cursor, ScanParams().count(count))
        cursor = resp.getCursor()
        result = resp.getResults()
        print(result)
        
        resp = self.client.sScan('key1', cursor, ScanParams().count(count))
        cursor = resp.getCursor()
        result = resp.getResults()
        print(result)
