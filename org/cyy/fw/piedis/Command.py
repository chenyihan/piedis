# -*- coding: GBK -*-
'''
Created on 2015年7月9日

@author: yunyun
'''
from builtins import isinstance
from org.cyy.fw.piedis.util import Decoder
from org.cyy.fw.piedis import RedisKeyword


PING = 'PING'
SET = 'SET'
GET = 'GET'
QUIT = 'QUIT'
EXISTS = 'EXISTS'
DEL = 'DEL'
TYPE = 'TYPE'
FLUSHDB = 'FLUSHDB'
KEYS = 'KEYS'
RANDOMKEY = 'RANDOMKEY'
RENAME = 'RENAME'
RENAMENX = 'RENAMENX'
RENAMEX = 'RENAMEX'
DBSIZE = 'DBSIZE'
EXPIRE = 'EXPIRE'
EXPIREAT = 'EXPIREAT' 
TTL = 'TTL' 
SELECT = 'SELECT' 
MOVE = 'MOVE'
FLUSHALL = 'FLUSHALL'
GETSET = 'GETSET'
MGET = 'MGET'
SETNX = 'SETNX'
SETEX = 'SETEX'
MSET = 'MSET'
MSETNX = 'MSETNX'
DECRBY = 'DECRBY'
DECR = 'DECR'
INCRBY = 'INCRBY'
INCR = 'INCR'
APPEND = 'APPEND'
SUBSTR = 'SUBSTR'
HSET = 'HSET'
HGET = 'HGET'
HSETNX = 'HSETNX'
HMSET = 'HMSET'
HMGET = 'HMGET'
HINCRBY = 'HINCRBY'
HEXISTS = 'HEXISTS'
HDEL = 'HDEL'
HLEN = 'HLEN'
HKEYS = 'HKEYS'
HVALS = 'HVALS'
HGETALL = 'HGETALL'
RPUSH = 'RPUSH'
LPUSH = 'LPUSH'
LLEN = 'LLEN'
LRANGE = 'LRANGE'
LTRIM = 'LTRIM'
LINDEX = 'LINDEX'
LSET = 'LSET'
LREM = 'LREM'
LPOP = 'LPOP'
RPOP = 'RPOP'
RPOPLPUSH = 'RPOPLPUSH' 
SADD = 'SADD'
SMEMBERS = 'SMEMBERS' 
SREM = 'SREM'
SPOP = 'SPOP'
SMOVE = 'SMOVE'
SCARD = 'SCARD'
SISMEMBER = 'SISMEMBER' 
SINTER = 'SINTER'
SINTERSTORE = 'SINTERSTORE' 
SUNION = 'SUNION'
SUNIONSTORE = 'SUNIONSTORE'
SDIFF = 'SDIFF'
SDIFFSTORE = 'SDIFFSTORE'
SRANDMEMBER = 'SRANDMEMBER'
ZADD = 'ZADD'
ZRANGE = 'ZRANGE'
ZREM = 'ZREM'
ZINCRBY = 'ZINCRBY'
ZRANK = 'ZRANK'
ZREVRANK = 'ZREVRANK'
ZREVRANGE = 'ZREVRANGE'
ZCARD = 'ZCARD'
ZSCORE = 'ZSCORE'
MULTI = 'MULTI'
DISCARD = 'DISCARD'
EXEC = 'EXEC'
WATCH = 'WATCH'
UNWATCH = 'UNWATCH'
SORT = 'SORT'
BLPOP = 'BLPOP'
BRPOP = 'BRPOP'
AUTH = 'AUTH'
SUBSCRIBE = 'SUBSCRIBE'
PUBLISH = 'PUBLISH'
UNSUBSCRIBE = 'UNSUBSCRIBE'
PSUBSCRIBE = 'PSUBSCRIBE'
PUNSUBSCRIBE = 'PUNSUBSCRIBE'
PUBSUB = 'PUBSUB'
ZCOUNT = 'ZCOUNT'
ZRANGEBYSCORE = 'ZRANGEBYSCORE'
ZREVRANGEBYSCORE = 'ZREVRANGEBYSCORE'
ZREMRANGEBYRANK = 'ZREMRANGEBYRANK'
ZREMRANGEBYSCORE = 'ZREMRANGEBYSCORE'
ZUNIONSTORE = 'ZUNIONSTORE'
ZINTERSTORE = 'ZINTERSTORE'
SAVE = 'SAVE'
BGSAVE = 'BGSAVE'
BGREWRITEAOF = 'BGREWRITEAOF' 
LASTSAVE = 'LASTSAVE'
SHUTDOWN = 'SHUTDOWN'
INFO = 'INFO'
MONITOR = 'MONITOR' 
SLAVEOF = 'SLAVEOF'
CONFIG = 'CONFIG'
STRLEN = 'STRLEN'
SYNC = 'SYNC'
LPUSHX = 'LPUSHX'
PERSIST = 'PERSIST'
RPUSHX = 'RPUSHX'
ECHO = 'ECHO'
LINSERT = 'LINSERT' 
DEBUG = 'DEBUG'
BRPOPLPUSH = 'BRPOPLPUSH'
SETBIT = 'SETBIT'
GETBIT = 'GETBIT'
BITPOS = 'BITPOS'
SETRANGE = 'SETRANGE'
GETRANGE = 'GETRANGE'
EVAL = 'EVAL'
EVALSHA = 'EVALSHA'
SCRIPT = 'SCRIPT'
SLOWLOG = 'SLOWLOG'
OBJECT = 'OBJECT'
BITCOUNT = 'BITCOUNT'
BITOP = 'BITOP'
SENTINEL = 'SENTINEL'
DUMP = 'DUMP'
RESTORE = 'RESTORE'
PEXPIRE = 'PEXPIRE'
PEXPIREAT = 'PEXPIREAT'
PTTL = 'PTTL'
INCRBYFLOAT = 'INCRBYFLOAT'
PSETEX = 'PSETEX'
CLIENT = 'CLIENT'
TIME = 'TIME'
MIGRATE = 'MIGRATE'
HINCRBYFLOAT = 'HINCRBYFLOAT'
SCAN = 'SCAN'
HSCAN = 'HSCAN'
SSCAN = 'SSCAN'
ZSCAN = 'ZSCAN'
WAIT = 'WAIT'
CLUSTER = 'CLUSTER'
ASKING = 'ASKING'
PFADD = 'PFADD'
PFCOUNT = 'PFCOUNT'
PFMERGE = 'PFMERGE'

class BinaryCommand:
    def __init__(self, command, *args):
        if isinstance(command, bytes):
            self.command = command
        else:
            self.command = Decoder.encodeData(command)
        if isinstance(args, bytes):
            self.args = args
            return
        if isinstance(args, str):
            self.args = Decoder.encodeData(args)
            return
        argsLen = len(args)
        self.args = [None] * argsLen
        for i in range(0, argsLen):
            if isinstance(args[i], bytes):
                self.args[i] = args[i]
            else:
                self.args[i] = Decoder.encodeData(str(args[i]))
        
    def getCommand(self):
        return self.command
    def getArgs(self):
        return self.args
    
class KeyValuePair:
    def __init__(self, key, value):
        self.key = key
        self.value = value
    
    def setKey(self, key):
        self.key = key
    
    def getKey(self):
        return self.key
    
    def setValue(self, value):
        self.value = value
    
    def getValue(self):
        return self.value

class ScoreMemberPair:
    def __init__(self, score, member):
        self.score = score
        self.member = member
    
    def getScore(self):
        return self.score
    
    def setScore(self, score):
        self.score = score
    
    def getMember(self):
        return self.member
    
    def setMember(self, member):
        self.member = member
    
    def __str__(self):
        return '[ScoreMemberPair]score:%s' % self.score + ';member:%s' % self.member
    
    def __repr__(self):
        return self.__str__()
        
class SortedSetParams:
    def __init__(self):
        self.params = []
    
    def weights(self, *weights):
        self.params.append(Decoder.decodeData(RedisKeyword.WEIGHTS))
        for weight in weights:
            self.params.append(str(weight))
        return self
    
    def aggregate(self, aggregate):
        self.params.append(Decoder.decodeData(RedisKeyword.AGGREGATE))
        self.params.append(Decoder.decodeData(aggregate));
        return self
    
    def getParams(self):
        return tuple(self.params)

class SortingParams():
    def __init__(self):
        self.params = []
    
    def by(self, pattern):
        self.params.append(RedisKeyword.BY)
        self.params.append(pattern)
        return self
    
    def nosort(self):
        self.params.append(RedisKeyword.BY)
        self.params.append(RedisKeyword.NOSORT)
        return self
    
    def desc(self):
        self.params.append(RedisKeyword.DESC)
        return self
    
    def asc(self):
        self.params.append(RedisKeyword.ASC)
        return self
    
    def limit(self, start, count):
        self.params.append(RedisKeyword.LIMIT)
        self.params.append(str(start))
        self.params.append(str(count))
        return self
    
    def alpha(self):
        self.params.append(RedisKeyword.ALPHA)
        return self
    
    def get(self, *patterns):
        for pattern in patterns:
            self.params.append(GET)
            self.params.append(pattern)
        return self
    
    def getParams(self):
        return self.params
    
class ScanParams:
    def __init__(self):
        self.params = []
    
    def getParams(self):
        return tuple(self.params)
    
    def match(self, pattern):
        self.params.append(RedisKeyword.MATCH)
        self.params.append(pattern)
        return self
    
    def count(self, count):
        self.params.append(RedisKeyword.COUNT)
        self.params.append(str(count))
        return self
    
def combineArgs(keyValuePair, *moreKeyValuePair):
    args = (keyValuePair.getKey(), keyValuePair.getValue())
    for pair in moreKeyValuePair:
        args += (pair.getKey(), pair.getValue())
    return args

def combineArgsByScoreMember(scoreMemberPair, *moreScoreMemberPair):
    args = (scoreMemberPair.getScore(), scoreMemberPair.getMember())
    for pair in moreScoreMemberPair:
        args += (pair.getScore(), pair.getMember())
    
    return args
