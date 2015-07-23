# -*- coding: GBK -*-
'''
Created on 2015年7月21日

@author: yunyun
'''
from org.cyy.fw.piedis.test import BaseCmdTest
from org.cyy.fw.piedis.Command import ScoreMemberPair, SortedSetParams, ScanParams
from org.cyy.fw.piedis import RedisKeyword
class SortSetCmdTest(BaseCmdTest):
    def test_zadd(self):
        self.client.flushAll()
        
        resp = self.client.zAdd('zkey1', ScoreMemberPair(2, 'zsvalue1'))
        self.assertEquals(resp, 1)
        
        resp = self.client.zAdd('zskey1', ScoreMemberPair(1, 'zsvalue2'), ScoreMemberPair(4, 'zsvalue1'))
        self.assertEquals(resp, 2)
    
    def test_zscore(self):
        self.client.flushAll()
        
        self.client.zAdd('zskey1', ScoreMemberPair(2.1, 'zsvalue1'), ScoreMemberPair(1.2, 'zsvalue2'), ScoreMemberPair(4.5, 'zsvalue3'))
        resp = self.client.zScore('zskey1', 'zsvalue11')
        self.assertEquals(resp, 0)
        resp = self.client.zScore('zskey1', 'zsvalue1')
        self.assertEquals(resp, 2.1)
        resp = self.client.zScore('zskey1', 'zsvalue2')
        self.assertEquals(resp, 1.2)
        resp = self.client.zScore('zskey1', 'zsvalue3')
        self.assertEquals(resp, 4.5)
        
    def test_zcard(self):
        self.client.flushAll()
        
        self.client.zAdd('zskey1', ScoreMemberPair(2.1, 'zsvalue1'), ScoreMemberPair(1.2, 'zsvalue2'), ScoreMemberPair(4.5, 'zsvalue3'))
        
        resp = self.client.zCard('zskey1')
        self.assertEquals(resp, 3)
        
    def test_zcount(self):
        self.client.flushAll()
        self.client.zAdd('zskey1', ScoreMemberPair(2.1, 'zsvalue1'), ScoreMemberPair(1.2, 'zsvalue2'), ScoreMemberPair(4.5, 'zsvalue3'))
        
        resp = self.client.zCount('zskey1', 0.1, 2.8)
        self.assertEquals(resp, 2)
    
    def test_zincrby(self):
        self.client.flushAll()
        
        resp = self.client.zIncrBy('zskey1', 'zsvalue1', 1.1)
        self.assertEquals(resp, 1.1)
        
        resp = self.client.zIncrBy('zskey1', 'zsvalue1', 2.1)
        self.assertEquals(resp, 3.2)
    
    def test_zrange(self):
        self.client.flushAll()
        self.client.zAdd('zskey1', ScoreMemberPair(2.1, 'zsvalue1'), ScoreMemberPair(1.2, 'zsvalue2'), ScoreMemberPair(4.5, 'zsvalue3'))
        
        resp = self.client.zRange('zskey1', 0, -1)
        self.assertTrue(isinstance(resp, tuple))
        print(resp)
        
        resp = self.client.zRangeWithScores('zskey1', 0, -1)
        self.assertTrue(isinstance(resp, tuple))
        print(resp)
        
    def test_zrangebyscore(self):
        self.client.flushAll()
        self.client.zAdd('zskey1', ScoreMemberPair(2.1, 'zsvalue1'), ScoreMemberPair(1.2, 'zsvalue2'), ScoreMemberPair(4.5, 'zsvalue3'))
        
        resp = self.client.zRangeByScore('zskey1', 2, 5)
        print(resp)
        
        resp = self.client.zRangeByScoreWithOffset('zskey1', 2, 5, 1, 1)
        print(resp)
        
        resp = self.client.zRevRangeWithScores('zskey1', 2, 5)
        print(resp)
        
        resp = self.client.zRevRangeByScoreWithScores('zskey1', 2, 5)
        print(resp)
        
        resp = self.client.zRevRangeByScoreWithScoresByOffset('zskey1', 2, 5, 1, 1)
        print(resp)
    
    def test_zrank(self):
        self.client.flushAll()
        self.client.zAdd("zskey1", ScoreMemberPair(2, "zsvalue1"), ScoreMemberPair(1, "zsvalue2"), ScoreMemberPair(4, "zsvalue3"), ScoreMemberPair(7, "zsvalue4"), ScoreMemberPair(8, "zsvalue5"))
        
        resp = self.client.zRank('zskey1', 'zsvalue1')
        self.assertEquals(resp, 1)
        resp = self.client.zRank('zskey1', 'zsvalue2')
        self.assertEquals(resp, 0)
        resp = self.client.zRank('zskey1', 'zsvalue3')
        self.assertEquals(resp, 2)
        resp = self.client.zRank('zskey1', 'zsvalue4')
        self.assertEquals(resp, 3)
        resp = self.client.zRank('zskey1', 'zsvalue5')
        self.assertEquals(resp, 4)
        resp = self.client.zRank('zskey1', 'zsvalue7')
        self.assertEquals(resp, -1)
    
    def test_zrem(self):
        self.client.flushAll()
        self.client.zAdd("zskey1", ScoreMemberPair(2, "zsvalue1"), ScoreMemberPair(1, "zsvalue2"), ScoreMemberPair(4, "zsvalue3"), ScoreMemberPair(7, "zsvalue4"), ScoreMemberPair(8, "zsvalue5"))
        
        resp = self.client.zRem('zskey1', 'zsvalue2', 'zsvalue1', 'zsvalue6')
        self.assertEquals(resp, 2)
        
        resp = self.client.zRange('zskey1', 0, -1)
        print(resp)
    
    def test_zremrangebyrank(self):
        self.client.flushAll()
        
        self.client.zAdd("zskey1", ScoreMemberPair(2, "zsvalue1"), ScoreMemberPair(1, "zsvalue2"), ScoreMemberPair(4, "zsvalue3"), ScoreMemberPair(7, "zsvalue4"), ScoreMemberPair(8, "zsvalue5"))
        resp = self.client.zRemRangeByRank('zskey1', 1, 3)
        self.assertEquals(resp, 3)
        
        resp = self.client.zRange('zskey1', 0, -1)
        print(resp)
    
    def test_zremrangebyscore(self):
        self.client.flushAll()
        
        self.client.zAdd("zskey1", ScoreMemberPair(2, "zsvalue1"), ScoreMemberPair(1, "zsvalue2"), ScoreMemberPair(4, "zsvalue3"), ScoreMemberPair(7, "zsvalue4"), ScoreMemberPair(8, "zsvalue5"))
        
        resp = self.client.zRemRangeByScore('zskey1', 4, 10)
        self.assertEquals(resp, 3)
        
        resp = self.client.zRange('zskey1', 0, -1)
        print(resp)
    
    def test_zrevrange(self):
        self.client.flushAll()
        
        self.client.zAdd("zskey1", ScoreMemberPair(2, "zsvalue1"), ScoreMemberPair(1, "zsvalue2"), ScoreMemberPair(4, "zsvalue3"), ScoreMemberPair(7, "zsvalue4"), ScoreMemberPair(8, "zsvalue5"))
        
        resp = self.client.zRevRange('zskey1', 0, -1)
        self.assertEquals(resp, ('zsvalue5', 'zsvalue4', 'zsvalue3', 'zsvalue1', 'zsvalue2'))
        
        resp = self.client.zRevRangeWithScores('zskey1', 0, -1)
        print(resp)
        
    def test_zRevRangeByScore(self):
        self.client.flushAll()
        
        self.client.zAdd("zskey1", ScoreMemberPair(2, "zsvalue1"), ScoreMemberPair(1, "zsvalue2"), ScoreMemberPair(4, "zsvalue3"), ScoreMemberPair(7, "zsvalue4"), ScoreMemberPair(8, "zsvalue5"))
        resp = self.client.zRevRangeByScore('zskey1', 8, 4)
        print(resp)
        
        resp = self.client.zRevRangeByScoreByOffset('zskey1', 8, 4, 2, 1)
        print(resp)
        
        resp = self.client.zRevRangeByScoreWithScores('zskey1', 8, 4)
        print(resp)
        
        resp = self.client.zRevRangeByScoreWithScoresByOffset('zskey1', 8, 4, 2, 1)
        print(resp)
        
    def test_zRevRank(self):
        self.client.flushAll()
        
        self.client.zAdd("zskey1", ScoreMemberPair(2, "zsvalue1"), ScoreMemberPair(1, "zsvalue2"), ScoreMemberPair(4, "zsvalue3"), ScoreMemberPair(7, "zsvalue4"), ScoreMemberPair(8, "zsvalue5"))
        
        resp = self.client.zRevRank('zskey1', 'zsvalue1')
        self.assertEquals(resp, 3)
        
        resp = self.client.zRevRank('zskey1', 'zsvalue2')
        self.assertEquals(resp, 4)
        
        resp = self.client.zRevRank('zskey1', 'zsvalue6')
        self.assertEquals(resp, -1)
    
    def test_zUnionStore(self):
        self.client.flushAll()
        
        self.client.zAdd("zskey1", ScoreMemberPair(52, "zsvalue1"), ScoreMemberPair(15, "zsvalue2"), ScoreMemberPair(34, "zsvalue3"), ScoreMemberPair(74, "zsvalue4"), ScoreMemberPair(18, "zsvalue5"))            
        self.client.zAdd("zskey2", ScoreMemberPair(32, "zsvalue21"), ScoreMemberPair(12, "zsvalue22"), ScoreMemberPair(14, "zsvalue3"), ScoreMemberPair(27, "zsvalue24"), ScoreMemberPair(28, "zsvalue25"))
        
        resp = self.client.zUnionStore('zskey3', None, 'zskey1', 'zskey2')
        self.assertEquals(resp, 9)
        
        resp = self.client.zRangeWithScores('zskey3', 0, -1)
        print(resp)
        
        resp = self.client.zUnionStore('zskey4', SortedSetParams().weights(1, 3), 'zskey1', 'zskey2')
        self.assertEquals(resp, 9)
        
        resp = self.client.zRangeWithScores('zskey4', 0, -1)
        print(resp)
        
        resp = self.client.zUnionStore('zskey5', SortedSetParams().weights(1, 3).aggregate(RedisKeyword.MAX), 'zskey1', 'zskey2')
        self.assertEquals(resp, 9)
        
        resp = self.client.zRangeWithScores('zskey5', 0, -1)
        print(resp)
    
    def test_zinterstore(self):
        self.client.flushAll()
        
        self.client.zAdd("zskey1", ScoreMemberPair(52, "zsvalue1"), ScoreMemberPair(15, "zsvalue2"), ScoreMemberPair(34, "zsvalue3"), ScoreMemberPair(74, "zsvalue4"), ScoreMemberPair(18, "zsvalue5"))            
        self.client.zAdd("zskey2", ScoreMemberPair(32, "zsvalue21"), ScoreMemberPair(12, "zsvalue22"), ScoreMemberPair(14, "zsvalue3"), ScoreMemberPair(27, "zsvalue24"), ScoreMemberPair(28, "zsvalue25"))
        
        resp = self.client.zInterStore('zskey3', None, 'zskey1', 'zskey2')
        self.assertEquals(resp, 1)
        
        resp = self.client.zRangeWithScores('zskey3', 0, -1)
        print(resp)
        
        resp = self.client.zInterStore('zskey4', SortedSetParams().weights(1, 3), 'zskey1', 'zskey2')
        self.assertEquals(resp, 1)
        
        resp = self.client.zRangeWithScores('zskey4', 0, -1)
        print(resp)
        
        resp = self.client.zInterStore('zskey5', SortedSetParams().weights(1, 3).aggregate(RedisKeyword.MIN), 'zskey1', 'zskey2')
        self.assertEquals(resp, 1)
        
        resp = self.client.zRangeWithScores('zskey5', 0, -1)
        print(resp)
    
    def test_zscan(self):
        self.client.flushAll()
        
        cursor = 0
        count = 5
        self.client.zAdd('key1', ScoreMemberPair(10, 'value1'))
        self.client.zAdd('key1', ScoreMemberPair(2, 'value2'))
        self.client.zAdd('key1', ScoreMemberPair(3, 'value3'))
        self.client.zAdd('key1', ScoreMemberPair(4, 'value4'))
        self.client.zAdd('key1', ScoreMemberPair(15, 'value5'))
        self.client.zAdd('key1', ScoreMemberPair(6, 'value6'))
        self.client.zAdd('key1', ScoreMemberPair(7, 'value7'))
        self.client.zAdd('key1', ScoreMemberPair(8, 'value8'))
        self.client.zAdd('key1', ScoreMemberPair(91, 'value9'))
        self.client.zAdd('key1', ScoreMemberPair(10, 'value10'))
        self.client.zAdd('key1', ScoreMemberPair(11, 'value11'))
        self.client.zAdd('key1', ScoreMemberPair(12, 'value12'))
        self.client.zAdd('key1', ScoreMemberPair(13, 'value13'))
        self.client.zAdd('key1', ScoreMemberPair(14, 'value14'))
        self.client.zAdd('key1', ScoreMemberPair(15, 'value15'))
        self.client.zAdd('key1', ScoreMemberPair(16, 'value16'))
        self.client.zAdd('key1', ScoreMemberPair(17, 'value17'))
        self.client.zAdd('key1', ScoreMemberPair(18, 'value18'))
        self.client.zAdd('key1', ScoreMemberPair(19, 'value19'))
        self.client.zAdd('key1', ScoreMemberPair(20, 'value20'))
        
        resp = self.client.zScan('key1', cursor, None)
        cursor = resp.getCursor()
        results = resp.getResults()
        print(results)
        
        resp = self.client.zScan('key1', cursor, ScanParams().count(count))
        cursor = resp.getCursor()
        results = resp.getResults()
        print(results)
        
        resp = self.client.zScan('key1', cursor, ScanParams().count(count))
        cursor = resp.getCursor()
        results = resp.getResults()
        print(results)
