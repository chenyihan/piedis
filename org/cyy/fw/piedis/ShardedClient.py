# -*- coding: GBK -*-
'''
Created on 2015年7月11日

@author: yunyun
'''
from org.cyy.fw.piedis.Server import NodeSharder
from org.cyy.fw.piedis.Client import PiedisClient
class ShardedClient:
    def __init__(self, nodes):
        self.nodes = nodes
        self.nodeSharder = NodeSharder(nodes)
        self.serverClientMapping = {}
        self.connectTimeout = 0
    
    def setConnectTimeout(self, timeout):
        self.connectTimeout = timeout
    
    def close(self):
        for node in self.nodes:
            client = self.getClientByNode(node)
            client.close()
            
    def flushAll(self):
        result = []
        for node in self.nodes:
            client = self.getClientByNode(node)
            resp = client.flushAll()
            result.append(resp)
        return tuple(result)
    
    def flushDB(self):
        result = []
        for node in self.nodes:
            client = self.getClientByNode(node)
            resp = client.flushDB()
            result.append(resp)
        return tuple(result)
    def expire(self, key, seconds):
        client = self.getClient(key)
        return client.expire(key, seconds)
    
    def expireAt(self, key, ts):
        client = self.getClient(key)
        return client.expireAt(key, ts)
                
    def move(self, key, destDb):
        client = self.getClient(key)
        return client.move(key, destDb)
    
    def objectIdletime(self, key):
        client = self.getClient(key)
        return client.objectIdletime(key)
    
    def objectEncoding(self, key):
        client = self.getClient(key)
        return client.objectEncoding(key)
    
    def objectRefcount(self, key):
        client = self.getClient(key)
        return client.objectRefcount(key)
    
    def persist(self, key):
        client = self.getClient(key)
        return client.persist(key)
    
    def pExpire(self, key, millSecs):
        client = self.getClient(key)
        return client.pExpire(key, millSecs)
    
    def pExpireAt(self, key, millTs):
        client = self.getClient(key)
        return client.pExpireAt(key, millTs)
    
    def pTTL(self, key):
        client = self.getClient(key)
        return client.pTTL(key)
    
    def sort(self, key, sortingParams):
        client = self.getClient(key)
        return client.sort(key, sortingParams, None)
    
    def ttl(self, key):
        client = self.getClient(key)
        return client.ttl(key)
    
    def type(self, key):
        client = self.getClient(key)
        return client.type(key)
    
    def set(self, key, value):
        client = self.getClient(key)
        return client.set(key, value)    
    
    def get(self, key):
        client = self.getClient(key)
        return client.get(key)
    
    def Del(self, key):
        client = self.getClient(key)
        return client.Del(key)
    
    def exists(self, key):
        client = self.getClient(key)
        return client.exists(key)
    
    def append(self, key, value):
        client = self.getClient(key)
        return client.append(key, value)
    
    def setBit(self, key, offset, value):
        client = self.getClient(key)
        return client.setBit(key, offset, value)
    
    def getBit(self, key, offset):
        client = self.getClient(key)
        return client.getBit(key, offset)
    
    def bitCount(self, key, start, end):
        client = self.getClient(key)
        return client.bitCount(key, start, end)
    
    def incr(self, key):
        client = self.getClient(key)
        return client.incr(key)
    
    def incrBy(self, key, increment):
        client = self.getClient(key)
        return client.incrBy(key, increment)
    
    def incrByFloat(self, key, increment):
        client = self.getClient(key)
        return client.incrByFloat(key, increment)
    
    def decr(self, key):
        client = self.getClient(key)
        return client.decr(key)
    
    def decrBy(self, key, increment):
        client = self.getClient(key)
        return client.decrBy(key, increment)
    
    def getRange(self, key, start, end):
        client = self.getClient(key)
        return client.getRange(key, start, end)
    
    def getSet(self, key, value):
        client = self.getClient(key)
        return client.getSet(key, value)
    
    def setNX(self, key, value):
        client = self.getClient(key)
        return client.setNX(key, value)
    
    def setEX(self, key, seconds, value):
        client = self.getClient(key)
        return client.setEX(key, seconds, value)
    
    def pSetEX(self, key, millSeconds, value):
        client = self.getClient(key)
        return client.pSetEX(key, millSeconds, value)
    
    def setRange(self, key, offset, value):
        client = self.getClient(key)
        return client.setRange(key, offset, value)
    
    def strLen(self, key):
        client = self.getClient(key)
        return client.strLen(key)
    
    def hSet(self, key, field, value):
        client = self.getClient(key)
        return client.hSet(key, field, value)
    
    def hSetNX(self, key, field, value):
        client = self.getClient(key)
        return client.hSetNX(key, field, value)
    
    def hGet(self, key, field):
        client = self.getClient(key)
        return client.hGet(key, field)
    
    def hGetAll(self, key):
        client = self.getClient(key)
        return client.hGetAll(key)
    
    def hDel(self, key, field):
        client = self.getClient(key)
        return client.hDel(key, field)
    
    def hExist(self, key, field):
        client = self.getClient(key)
        return client.hExist(key, field)
    
    def hIncrBy(self, key, field, increment):
        client = self.getClient(key)
        return client.hIncrBy(key, field, increment)
    
    def hIncrByFloat(self, key, field, increment):
        client = self.getClient(key)
        return client.hIncrByFloat(key, field, increment)
    
    def hKeys(self, key):
        client = self.getClient(key)
        return client.hKeys(key)
    
    def hVals(self, key):
        client = self.getClient(key)
        return client.hVals(key)
    
    def hLen(self, key):
        client = self.getClient(key)
        return client.hLen(key)
    
    def lPush(self, key, value, *moreValues):
        client = self.getClient(key)
        return client.lPush(key, value, *moreValues)
    
    def lpushX(self, key, value):
        client = self.getClient(key)
        return client.lPushX(key, value)
    
    def lpop(self, key):
        client = self.getClient(key)
        return client.lPop(key)
    
    def rpush(self, key, value, *moreValues):
        client = self.getClient(key)
        return client.rPush(key, value, *moreValues)
    
    def rpushX(self, key, value):
        client = self.getClient(key)
        return client.rPushX(key, value)
    
    def rpop(self, key):
        client = self.getClient(key)
        return client.rPop(key)
    
    def bLpop(self, key, timeout):
        client = self.getClient(key)
        return client.blPop(timeout, key)
    
    def bRpop(self, key, timeout):
        client = self.getClient(key)
        return client.brPop(timeout, key)
    
    def lRange(self, key, start, end):
        client = self.getClient(key)
        return client.lRange(key, start, end)
    
    def lIndex(self, key, index):
        client = self.getClient(key)
        return client.lIndex(key, index)
    
    def lInsert(self, key, value, pivot, isBefore):
        client = self.getClient(key)
        return client.lInsert(key, value, pivot, isBefore)
    
    def lLen(self, key):
        client = self.getClient(key)
        return client.lLen(key)
    
    def lREM(self, key, value, count):
        client = self.getClient(key)
        return client.lREM(key, value, count)
    
    def lSet(self, key, index, value):
        client = self.getClient(key)
        return client.lSet(key, index, value)
    
    def lTrim(self, key, start, end):
        client = self.getClient(key)
        return client.lTrim(key, start, end)
    
    def sAdd(self, key, member, *moreMembers):
        client = self.getClient(key)
        return client.sAdd(key, member, *moreMembers)
    
    def sCard(self, key):
        client = self.getClient(key)
        return client.sCard(key)
    
    def sPop(self, key):
        client = self.getClient(key)
        return client.sPop(key)
    
    def sRandMember(self, key, count):
        client = self.getClient(key)
        return client.sRandMember(key, count)
    
    def sMembers(self, key):
        client = self.getClient(key)
        return client.sMembers(key)
    
    def sisMember(self, key, member):
        client = self.getClient(key)
        return client.sisMember(key, member)
    
    def sRem(self, key, member):
        client = self.getClient(key)
        return client.sRem(key, member)
    
    def zAdd(self, key, scoreMember, *scoreMembers):
        client = self.getClient(key)
        return client.zAdd(key, scoreMember, *scoreMembers)
    
    def zCard(self, key):
        client = self.getClient(key)
        return client.zCard(key)
    
    def zCount(self, key, Min, Max):
        client = self.getClient(key)
        return client.zCount(key, Min, Max)
    
    def zIncrBy(self, key, member, increment):
        client = self.getClient(key)
        return client.zIncrBy(key, member, increment)
    
    def zRange(self, key, start, end):
        client = self.getClient(key)
        return client.zRange(key, start, end)
    
    def zRangeWithScores(self, key, start, end):
        client = self.getClient(key)
        return client.zRangeWithScores(key, start, end)
    
    def zRangeByScore(self, key, Min, Max):
        client = self.getClient(key)
        return client.zRangeByScore(key, Min, Max)
    
    def zRangeByScoreWithOffset(self, key, Min, Max, offset, count):
        client = self.getClient(key)
        return client.zRangeByScoreWithOffset(key, Min, Max, offset, count)
    
    def zRangeByScoreWithScores(self, key, Min, Max):
        client = self.getClient(key)
        return client.zRangeByScoreWithScores(key, Min, Max)
    
    def zRangeByScoreWithScoresByOffset(self, key, Min, Max, offset, count):
        client = self.getClient(key)
        return client.zRangeByScoreWithScoresByOffset(key, Min, Max, offset, count)
    
    def zRank(self, key, member):
        client = self.getClient(key)
        return client.zRank(key, member)
    
    def zRem(self, key, member, *members):
        client = self.getClient(key)
        return client.zRem(key, member, *members)
    
    def zRemRangeByRank(self, key, start, end):
        client = self.getClient(key)
        return client.zRemRangeByRank(key, start, end)
    
    def zRemRangeByScore(self, key, Min, Max):
        client = self.getClient(key)
        return client.zRemRangeByScore(key, Min, Max)
    
    def zRevRange(self, key, start, end):
        client = self.getClient(key)
        return client.zRevRange(key, start, end)
    
    def zRevRangeWithScores(self, key, start, end):
        client = self.getClient(key)
        return client.zRevRangeWithScores(key, start, end)
    
    def zRevRangeByScore(self, key, Max, Min):
        client = self.getClient(key)
        return client.zRevRangeByScore(key, Max, Min)
    
    def zRevRangeByScoreByOffset(self, key, Max, Min, offset, count):
        client = self.getClient(key)
        return client.zRevRangeByScoreByOffset(key, Max, Min, offset, count)
    
    def zRevRangeByScoreWithScores(self, key, Max, Min):
        client = self.getClient(key)
        return client.zRevRangeByScoreWithScores(key, Max, Min)
    
    def zRevRangeByScoreWithScoresByOffset(self, key, Max, Min, offset, count):
        client = self.getClient(key)
        return client.zRevRangeByScoreWithScoresByOffset(key, Max, Min, offset, count)
    
    def zRevRank(self, key, start, end):
        client = self.getClient(key)
        return client.zRevRank(key, start, end)
    
    def zScore(self, key, member):
        client = self.getClient(key)
        return client.zScore(key, member)
    
    def echo(self, message):
        client = self.getClient(message)
        return client.echo(message)
    
    def getClient(self, key):
        shardNodeInfo = self.nodeSharder.getShardNodeInfo(key)
        print('shardNodeInfo %s' % shardNodeInfo)
        return self.getClientByNode(shardNodeInfo)
    
    def getClientByNode(self, shardNodeInfo):
        client = self.serverClientMapping.get(shardNodeInfo)
        if client != None:
            return client
        client = PiedisClient()
        client.setHost(shardNodeInfo.getHost())
        client.setPort(shardNodeInfo.getPort())
        client.setConnectTimeout(self.connectTimeout)
        self.serverClientMapping[shardNodeInfo] = client
        return client
        
