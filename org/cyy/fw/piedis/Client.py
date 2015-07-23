# -*- coding: GBK -*-
'''
Created on 2015年7月8日

@author: yunyun
'''
import socket

from org.cyy.fw.piedis import RedisProtocol, Command, RedisKeyword, Response
from org.cyy.fw.piedis.Command import BinaryCommand
from org.cyy.fw.piedis.Server import ServerNode


class PiedisClient:
    def __init__(self):
        self.isConnect = False
        self.sock = None
        self.serverNode = ServerNode()
        self.connectTimeout = 0
    
    def setHost(self, host):
        self.serverNode.setHost(host)
        return self
    
    def setPort(self, port):
        self.serverNode.setPort(port)
        return self
    
    def setConnectTimeout(self, timeout):
        self.connectTimeout = timeout
    
    def dump(self, key):
        resp = self.sendCommand(Command.DUMP, key)
        return resp
    
    def expire(self, key, seconds):
        resp = self.sendCommand(Command.EXPIRE, key, str(seconds))
        return Response.toInt(resp) == 1
    
    def expireAt(self, key, ts):
        resp = self.sendCommand(Command.EXPIREAT, key, str(ts))
        return Response.toInt(resp) == 1
    
    def migrate(self, key, destHost, destPort, destDb, timeout):
        resp = self.sendCommand(Command.MIGRATE, key, str(destHost), str(destPort), str(destDb), str(timeout))
        return Response.toStr(resp)
    
    def move(self, key, destDb):
        resp = self.sendCommand(Command.MOVE, key, str(destDb))
        return Response.toInt(resp) == 1
    
    def objectRefcount(self, key):
        resp = self.sendCommand(Command.OBJECT, RedisKeyword.REFCOUNT, key)
        return Response.toInt(resp)
        
    def objectIdletime(self, key):
        resp = self.sendCommand(Command.OBJECT, RedisKeyword.IDLETIME, key)
        return Response.toInt(resp)
    
    def objectEncoding(self, key):
        resp = self.sendCommand(Command.OBJECT, RedisKeyword.ENCODING, key)
        return Response.toStr(resp)
    
    def persist(self, key):
        resp = self.sendCommand(Command.PERSIST, key)
        return Response.toInt(resp) == 1
    
    def pExpire(self, key, millSeconds):
        resp = self.sendCommand(Command.PEXPIRE, key, str(millSeconds))
        return Response.toInt(resp) == 1
    
    def pExpireAt(self, key, millTs):
        resp = self.sendCommand(Command.PEXPIREAT, key, str(millTs))
        return Response.toInt(resp) == 1
    
    def pTTL(self, key):
        resp = self.sendCommand(Command.PTTL, key)
        return Response.toInt(resp)
    
    def randomKey(self):
        resp = self.sendCommand(Command.RANDOMKEY)
        return Response.toStr(resp)
    
    def rename(self, key, newKey):
        resp = self.sendCommand(Command.RENAME, key, newKey)
        return Response.toStr(resp)
    
    def renameNX(self, key, newKey):
        resp = self.sendCommand(Command.RENAMENX, key, newKey)
        return Response.toInt(resp) == 1
    
    def restore(self, key, serialValue, millTTL, replace):
        if replace :
            resp = self.sendCommand(Command.RESTORE, key, str(millTTL), serialValue, RedisKeyword.REPLACE)
            return Response.toStr(resp)
        else:
            resp = self.sendCommand(Command.RESTORE, key, str(millTTL), serialValue)
            return Response.toStr(resp)
    
    def sort(self, key, params, destKey):
        args = []
        args.append(key)
        if params != None:
            args += params.getParams()
        if destKey != None:
            args.append(RedisKeyword.STORE)
            args.append(destKey)
        
        resp = self.sendCommand(Command.SORT, *tuple(args))
        return Response.toTuple(resp)
        
    def ttl(self, key):
        resp = self.sendCommand(Command.TTL, key)
        return Response.toInt(resp)
    
    def type(self, key):
        resp = self.sendCommand(Command.TYPE, key)
        return Response.toStr(resp)
    
    def scan(self, cursor, params):
        args = (str(cursor),)
        if params != None:
            args = args + params.getParams()
        
        resp = self.sendCommand(Command.SCAN, *args)
        return Response.toScanResult(resp)
        
    def set(self, key, value):
        resp = self.sendCommand(Command.SET, key, str(value))
        return Response.toStr(resp)
    
    def get(self, key):
        resp = self.sendCommand(Command.GET, key)
        return Response.toStr(resp)
    
    def Del(self, key, *moreKeys):
        resp = self.sendCommand(Command.DEL, key, *moreKeys)
        return Response.toInt(resp)
    
    def keys(self, pattern):
        resp = self.sendCommand(Command.KEYS, pattern)
        return Response.toTuple(resp)
    
    def exists(self, key):
        resp = self.sendCommand(Command.EXISTS, key)
        return Response.toInt(resp) == 1
    
    def append(self, key, value):
        resp = self.sendCommand(Command.APPEND, key, value)
        return Response.toInt(resp)
    
    def setBit(self, key, offset, value):
        resp = self.sendCommand(Command.SETBIT, key, str(offset), value)
        return Response.toInt(resp)
    
    def getBit(self, key, offset):
        resp = self.sendCommand(Command.GETBIT, key, str(offset))
        return Response.toInt(resp)
    
    def bitCount(self, key, start, end):
        args = [key]
        if start != None:
            args.append(str(start))
            if end != None:
                args.append(str(end))
        resp = self.sendCommand(Command.BITCOUNT, *tuple(args))
        return Response.toInt(resp)
    
    def bitop(self, bitOP, destKey, srcKey, *moreSrcKeys):
        resp = self.sendCommand(Command.BITOP, bitOP, destKey, srcKey, *moreSrcKeys)
        return Response.toInt(resp)
    
    def incr(self, key):
        resp = self.sendCommand(Command.INCR, key)
        return Response.toInt(resp)
    
    def incrBy(self, key, increment):
        resp = self.sendCommand(Command.INCRBY, key, str(increment))
        return Response.toInt(resp)
    
    def incrByFloat(self, key, increment):
        resp = self.sendCommand(Command.INCRBYFLOAT, key, str(increment))
        return Response.toFloat(resp)
    
    def decr(self, key):
        resp = self.sendCommand(Command.DECR, key)
        return Response.toInt(resp)
    
    def decrBy(self, key, increment):
        resp = self.sendCommand(Command.DECRBY, key, str(increment))
        return Response.toInt(resp)
    
    def getRange(self, key, start, end):
        resp = self.sendCommand(Command.GETRANGE, key, str(start), str(end))
        return Response.toStr(resp)
    
    def getSet(self, key, value):
        resp = self.sendCommand(Command.GETSET, key, value)
        return Response.toStr(resp)
    
    def mSet(self, keyValuePair, *moreKeyValuePair):
        args = Command.combineArgs(keyValuePair, *moreKeyValuePair)
        resp = self.sendCommand(Command.MSET, *args)
        return Response.toStr(resp)
    
    def mGet(self, key, *moreKeys):
        resp = self.sendCommand(Command.MGET, key, *moreKeys)
        return Response.toTuple(resp)
    
    def mSetNX(self, keyValuePair, *moreKeyValuePair):
        args = Command.combineArgs(keyValuePair, *moreKeyValuePair)
        resp = self.sendCommand(Command.MSETNX, *args)
        return Response.toInt(resp) == 1
    
    def setNX(self, key, value):
        resp = self.sendCommand(Command.SETNX, key, value)
        return Response.toInt(resp) == 1
    
    def setEX(self, key, seconds, value):
        resp = self.sendCommand(Command.SETEX, key, str(seconds), value)
        return Response.toStr(resp)
    
    def pSetEX(self, key, millSeconds, value):
        resp = self.sendCommand(Command.PSETEX, key, str(millSeconds), value)
        return Response.toStr(resp)
    
    def setRange(self, key, offset, value):
        resp = self.sendCommand(Command.SETRANGE, key, str(offset), value)
        return Response.toInt(resp)
    
    def strLen(self, key):
        resp = self.sendCommand(Command.STRLEN, key)
        return Response.toInt(resp)
    
    def hSet(self, key, field, value):
        resp = self.sendCommand(Command.HSET, key, field, value)
        return Response.toInt(resp) == 1
    
    def hSetNX(self, key, field, value):
        resp = self.sendCommand(Command.HSETNX, key, field, value)
        return Response.toInt(resp) == 1
    
    def hGet(self, key, field):
        resp = self.sendCommand(Command.HGET, key, field)
        return Response.toStr(resp)
    
    def hGetAll(self, key):
        resp = self.sendCommand(Command.HGETALL, key)
        return Response.toDict(resp)
    
    def hDel(self, key, field):
        resp = self.sendCommand(Command.HDEL, key, field)
        return Response.toInt(resp)
    
    def hExist(self, key, field):
        resp = self.sendCommand(Command.HEXISTS, key, field)
        return Response.toInt(resp) == 1
    
    def hIncrBy(self, key, field, increment):
        resp = self.sendCommand(Command.HINCRBY, key, field, str(increment))
        return Response.toInt(resp)
    
    def hIncrByFloat(self, key, field, increment):
        resp = self.sendCommand(Command.HINCRBYFLOAT, key, field, str(increment))
        return Response.toFloat(resp)
    
    def hKeys(self, key):
        resp = self.sendCommand(Command.HKEYS, key)
        return Response.toTuple(resp)
    
    def hVals(self, key):
        resp = self.sendCommand(Command.HVALS, key)
        return Response.toTuple(resp)
    
    def hLen(self, key):
        resp = self.sendCommand(Command.HLEN, key)
        return Response.toInt(resp)
    
    def hMSet(self, key, keyValuePair, *moreKeyValuePairs):
        args = Command.combineArgs(keyValuePair, *moreKeyValuePairs)
        resp = self.sendCommand(Command.HMSET, key, *args)
        return Response.toStr(resp)
    
    def hMGet(self, key, field, *moreFields):
        resp = self.sendCommand(Command.HMGET, key, field, *moreFields)
        return Response.toTuple(resp)
    
    
    '''
    The hscan command seem to be a bug
    '''
    def hScan(self, key, cursor, scanParams):
        args = (key, cursor)
        if scanParams != None:
            args += scanParams.getParams()
        resp = self.sendCommand(Command.HSCAN, *args)
        return Response.toHashScanResult(resp)
    
    def lPush(self, key, value, *moreValues):
        resp = self.sendCommand(Command.LPUSH, key, value, *moreValues)
        return Response.toInt(resp)
    
    def lPushX(self, key, value):
        resp = self.sendCommand(Command.LPUSHX, key, value)
        return Response.toInt(resp)
    
    def lPop(self, key):
        resp = self.sendCommand(Command.LPOP, key)
        return Response.toStr(resp)
    
    def rPush(self, key, value, *moreValues):
        resp = self.sendCommand(Command.RPUSH, key, value, *moreValues)
        return Response.toInt(resp)
    
    def rPushX(self, key, value):  
        resp = self.sendCommand(Command.RPUSHX, key, value)
        return Response.toInt(resp)     
    
    def rPop(self, key):
        resp = self.sendCommand(Command.RPOP, key)
        return Response.toStr(resp)
    
    def blPop(self, timeout, key, *morekeys):
        args = (key,)
        args += morekeys
        args += (timeout,)
        resp = self.sendCommand(Command.BLPOP, *args)
        return Response.toTuple(resp)
    
    def brPop(self, timeout, key, *morekeys):
        args = (key,)
        args += morekeys
        args += (timeout,)
        resp = self.sendCommand(Command.BRPOP, *args)
        return Response.toTuple(resp)
    
    def lRange(self, key, start, end):
        resp = self.sendCommand(Command.LRANGE, key, str(start), str(end))
        return Response.toTuple(resp)
    
    def rPopLPush(self, sourceKey, destKey):
        resp = self.sendCommand(Command.RPOPLPUSH, sourceKey, destKey)
        return Response.toStr(resp)
    
    def bRPopLPush(self, sourceKey, destKey, timeout):
        resp = self.sendCommand(Command.BRPOPLPUSH, sourceKey, destKey, str(timeout))
        return Response.toStr(resp)
    
    def lIndex(self, key, index):
        resp = self.sendCommand(Command.LINDEX, key, str(index))
        return Response.toStr(resp)
    
    def lInsert(self, key, value, pivot, isBefore):
        extraParam = RedisKeyword.BEFORE
        if not isBefore:
            extraParam = RedisKeyword.AFTER
        resp = self.sendCommand(Command.LINSERT, key, extraParam, pivot, value)
        return Response.toInt(resp)
    
    def lLen(self, key):
        resp = self.sendCommand(Command.LLEN, key)
        return Response.toInt(resp)
    
    def lREM(self, key, value, count):
        resp = self.sendCommand(Command.LREM, key, count, value)
        return Response.toInt(resp)
    
    def lSet(self, key, index, value):
        resp = self.sendCommand(Command.LSET, key, index, value)
        return Response.toStr(resp)
    
    def lTrim(self, key, start, end):
        resp = self.sendCommand(Command.LTRIM, key, str(start), str(end))
        return Response.toStr(resp)
    
    def sAdd(self, key, member, *moreMember):
        resp = self.sendCommand(Command.SADD, key, member, *moreMember)
        return Response.toInt(resp)
    
    def sCard(self, key):
        resp = self.sendCommand(Command.SCARD, key)
        return Response.toInt(resp)
    
    def sPop(self, key):
        resp = self.sendCommand(Command.SPOP, key)
        return Response.toStr(resp)
    
    def sRandMember(self, key, count):
        resp = self.sendCommand(Command.SRANDMEMBER, key, str(count))
        return Response.toTuple(resp)
    
    def sMembers(self, key):
        resp = self.sendCommand(Command.SMEMBERS, key)
        return Response.toTuple(resp)
    
    def sDiff(self, key, *moreKeys):
        resp = self.sendCommand(Command.SDIFF, key, *moreKeys)
        return Response.toTuple(resp)
    
    def sDiffStore(self, destKey, key, *moreKeys):
        resp = self.sendCommand(Command.SDIFFSTORE, destKey, key, *moreKeys)
        return Response.toInt(resp)
    
    def sInter(self, key, *moreKeys):
        resp = self.sendCommand(Command.SINTER, key, *moreKeys)
        return Response.toTuple(resp)
    
    def sInterStore(self, destKey, key, *moreKeys):
        resp = self.sendCommand(Command.SINTERSTORE, destKey, key, *moreKeys)
        return Response.toInt(resp)
    
    def sisMember(self, key, member):
        resp = self.sendCommand(Command.SISMEMBER, key, member)
        return Response.toInt(resp) == 1
    
    def sRem(self, key, member, *moreMembers):
        resp = self.sendCommand(Command.SREM, key, member, *moreMembers)
        return Response.toInt(resp)
    
    def sMove(self, source, destination, member):
        resp = self.sendCommand(Command.SMOVE, source, destination, member)
        return Response.toInt(resp) == 1
    
    def sUnion(self, key, *moreKeys):
        resp = self.sendCommand(Command.SUNION, key, *moreKeys)
        return Response.toTuple(resp)
    
    def sUnionStore(self, destKey, key, *moreKeys):
        resp = self.sendCommand(Command.SUNIONSTORE, destKey, key, *moreKeys)
        return Response.toInt(resp)
    
    def sScan(self, key, cursor, scanParams):
        args = (key, cursor)
        if scanParams != None:
            args = args + scanParams.getParams()
        resp = self.sendCommand(Command.SSCAN, *args)
        return Response.toStringScanResult(resp)
    
    def zAdd(self, key, scoreMember, *moreScoreMember):
        args = Command.combineArgsByScoreMember(scoreMember, *moreScoreMember)
        resp = self.sendCommand(Command.ZADD, key, *args)
        return Response.toInt(resp)
    
    def zCard(self, key):
        resp = self.sendCommand(Command.ZCARD, key)
        return Response.toInt(resp)
    
    def zCount(self, key, Min, Max):
        resp = self.sendCommand(Command.ZCOUNT, key, str(Min), str(Max))
        return Response.toInt(resp)
    
    def zIncrBy(self, key, member, increment):
        resp = self.sendCommand(Command.ZINCRBY, key, increment, member)
        return Response.toFloat(resp)
    
    def zRange(self, key, start, end):
        resp = self.sendCommand(Command.ZRANGE, key, str(start), str(end))
        return Response.toTuple(resp)
    
    def zRangeWithScores(self, key, start, end):
        resp = self.sendCommand(Command.ZRANGE, key, str(start), str(end), RedisKeyword.WITHSCORES)
        return Response.toScoreMember(resp)
    
    def zRangeByScore(self, key, Min, Max):
        resp = self.sendCommand(Command.ZRANGEBYSCORE, key, str(Min), str(Max))
        return Response.toTuple(resp)
    
    def zRangeByScoreWithOffset(self, key, Min, Max, offset, count):
        resp = self.sendCommand(Command.ZRANGEBYSCORE, key, str(Min), str(Max), RedisKeyword.LIMIT, str(offset), str(count))
        return Response.toTuple(resp)
    
    def zRangeByScoreWithScores(self, key, Min, Max):
        resp = self.sendCommand(Command.ZRANGEBYSCORE, key, str(Min), str(Max), RedisKeyword.WITHSCORES)
        return Response.toScoreMember(resp)
    
    def zRangeByScoreWithScoresByOffset(self, key, Min, Max, offset, count):
        resp = self.sendCommand(Command.ZRANGEBYSCORE, key, str(Min), str(Max), RedisKeyword.WITHSCORES), RedisKeyword.LIMIT, str(offset), str(count)
        return Response.toScoreMember(resp)
    
    def zRank(self, key, member):
        resp = self.sendCommand(Command.ZRANK, key, member)
        return Response.toInt(resp)
    
    def zRem(self, key, member, *moreMembers):
        resp = self.sendCommand(Command.ZREM, key, member, *moreMembers)
        return Response.toInt(resp)
    
    def zRemRangeByRank(self, key, start, end):
        resp = self.sendCommand(Command.ZREMRANGEBYRANK, key, str(start), str(end))
        return Response.toInt(resp)
    
    def zRemRangeByScore(self, key, Min, Max):
        resp = self.sendCommand(Command.ZREMRANGEBYSCORE, key, str(Min), str(Max))
        return Response.toInt(resp)
    
    def zRevRange(self, key, start, end):
        resp = self.sendCommand(Command.ZREVRANGE, key, str(start), str(end))
        return Response.toTuple(resp)
    
    def zRevRangeWithScores(self, key, start, end):
        resp = self.sendCommand(Command.ZREVRANGE, key, str(start), str(end), RedisKeyword.WITHSCORES)
        return Response.toScoreMember(resp)
    
    def zRevRangeByScore(self, key, Max, Min):
        resp = self.sendCommand(Command.ZREVRANGEBYSCORE, key, str(Max), str(Min))
        return Response.toTuple(resp)
    
    def zRevRangeByScoreByOffset(self, key, Max, Min, offset, count):
        resp = self.sendCommand(Command.ZREVRANGEBYSCORE, key, str(Max), str(Min), RedisKeyword.LIMIT, str(offset), str(count))
        return Response.toTuple(resp)
    
    def zRevRangeByScoreWithScores(self, key, Max, Min):
        resp = self.sendCommand(Command.ZREVRANGEBYSCORE, key, str(Max), str(Min), RedisKeyword.WITHSCORES)
        return Response.toScoreMember(resp)
    
    def zRevRangeByScoreWithScoresByOffset(self, key, Max, Min, offset, count):
        resp = self.sendCommand(Command.ZREVRANGEBYSCORE, key, str(Max), str(Min), RedisKeyword.LIMIT, str(offset), str(count))
        return Response.toScoreMember(resp)
    
    def zRevRank(self, key, member):
        resp = self.sendCommand(Command.ZREVRANK, key, member)
        return Response.toInt(resp)
    
    def zScore(self, key, member):
        resp = self.sendCommand(Command.ZSCORE, key, member)
        return Response.toFloat(resp)
    
    def zUnionStore(self, destKey, params, key, *moreKeys):
        args = (destKey,)
        numberKeys = 1 + len(moreKeys)
        args += (numberKeys, key)
        args += moreKeys
        if params != None:
            args += params.getParams()
        resp = self.sendCommand(Command.ZUNIONSTORE, *args)
        return Response.toInt(resp)
        
    def zInterStore(self, destKey, params, key, *moreKeys):
        args = (destKey,)
        numberKeys = 1 + len(moreKeys)
        args += (numberKeys, key)
        args += moreKeys
        if params != None:
            args += params.getParams()
        
        resp = self.sendCommand(Command.ZINTERSTORE, *args)
        return Response.toInt(resp)
    
    '''
    The zscan command seem to be a bug
    '''
    def zScan(self, key, cursor, scanParams):
        args = (key, cursor)
        if scanParams != None:
            args += scanParams.getParams()
        resp = self.sendCommand(Command.ZSCAN, *args)
        return Response.toScoreMemberScanResult(resp)
    
    def select(self, index):
        resp = self.sendCommand(Command.SELECT, str(index))
        return Response.toStr(resp)
    
    def auth(self, password):
        resp = self.sendCommand(Command.AUTH, password)
        return Response.toStr(resp)
    
    def echo(self, message):
        resp = self.sendCommand(Command.ECHO, message)
        return Response.toStr(resp)
    
    def ping(self):
        resp = self.sendCommand(Command.PING)
        return Response.toStr(resp)
    
    def quit(self):
        resp = self.sendCommand(Command.QUIT)
        return Response.toStr(resp)
    
    def flushAll(self):
        resp = self.sendCommand(Command.FLUSHALL)
        return Response.toStr(resp)
    
    def flushDB(self):
        resp = self.sendCommand(Command.FLUSHDB)
        return Response.toStr(resp)
        
    
    def sendCommand(self, command, *args):
        message = BinaryCommand(command, *args)
        data = RedisProtocol.generateRequestData(message)
        self.connect() 
#         print('send command:')
#         print(Decoder.decodeData(data))
        try:
            self.sock.send(data)
        except:
            self.isConnect = False
            self.sendCommand(command, *args)
        resp = RedisProtocol.parseResponse(self.sock)
        print('response:', resp)
        return resp
        
    
    def connect(self):
        if self.isConnect:
            return;
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.connectTimeout > 0:
            self.sock.settimeout(self.connectTimeout)
        self.sock.connect((self.serverNode.getHost(), self.serverNode.getPort()))
        self.isConnect = True
    
    def close(self):
        if not self.isConnect:
            return;
        self.isConnect = False
        self.sock.close()
    
    
