# -*- coding: GBK -*-
'''
Created on 2015年7月18日

@author: yunyun
'''
import collections

from org.cyy.fw.piedis.util import Decoder
from org.cyy.fw.piedis.Command import ScoreMemberPair


class ScanResult:
    def __init__(self, cursor, results):
        self.cursor = cursor
        self.results = results
        
    def getCursor(self):
        return self.cursor
    
    def getResults(self):
        return self.results
        

def toStr(resp):
        if isinstance(resp, bytes):
            return Decoder.decodeData(resp)
        else:
            return str(resp)
    
def toInt(resp):
    s = toStr(resp)
    if s == 'null':
        s = -1
    return int(s)

def toFloat(resp):
    s = toStr(resp)
    if s == 'null':
        s = 0
    return float(s)

def toTuple(resp):
    if not isinstance(resp, collections.Iterable):
        return (toStr(resp))
    result = []
    for ele in resp:
        result.append(toStr(ele))
    
    return tuple(result)

def toDict(resp):
    result = {}
    if resp == None:
        return result
    
    if not isinstance(resp, collections.Iterable):
        result[_] = toStr(resp)
        return result
    
    l = len(resp)
    for i in range(0, l, 2):
        field = resp[i]
        value = None
        if i < l - 1:
            value = resp[i + 1]
        field = toStr(field)
        value = toStr(value)
        result[field] = value
    return result

def toScoreMember(resp):
    if resp == None:
        return ()
    if not isinstance(resp, collections.Iterable):
        return (toStr(resp),)
    Len = len(resp)
    result = []
    for i in range(0, Len, 2):
        member = resp[i]
        score = 0.0
        if i < Len - 1:
            score = resp[i + 1]
        if i % 2 == 0:
            result.append(ScoreMemberPair(toFloat(score), toStr(member)))
#             result[i >> 1] = ScoreMemberPair(toFloat(score), toStr(member))
    return tuple(result)
    

def toScanResult(resp):
    newcursor = resp[0]
    newcursor = toInt(newcursor)
    rawResults = resp[1]
    results = []
    i = 0
    for r in rawResults:
        results.append(toStr(r))
        i += 1
    
    return ScanResult(newcursor, results)

def toHashScanResult(resp):
    if resp == None:
        return ScanResult(0, (None, None))
    if not isinstance(resp, collections.Iterable):
        respStr = toStr(resp)
        return ScanResult(0, (respStr, respStr))
    newCursor = toStr(resp[0])
    if len(resp) == 1:
        return ScanResult(newCursor, (None, None))
    
    rawResults = resp[1]
    results = []
    Len = len(rawResults)
    for i in range(0, Len, 2):
        field = rawResults[i]
        value = None
        if i < Len - 1:
            value = rawResults[i + 1]
        results.append((toStr(field), toStr(value)))
    
    return ScanResult(newCursor, results)

def toStringScanResult(resp):
    if resp == None:
        return ScanResult(0, 'null')
    if not isinstance(resp, collections.Iterable):
        return ScanResult(0, toStr(resp))
    newCursor = resp[0]
    if len(resp) == 1:
        return ScanResult(newCursor, 'null')
    rawResults = resp[1]
    results = []
    Len = len(rawResults)
    for i in range(0, Len):
        results.append(toStr(rawResults[i]))
    return ScanResult(newCursor, results)

def toScoreMemberScanResult(resp):
    if resp == None:
        return ScanResult(0, [])
    if not isinstance(resp, collections.Iterable):
        return ScanResult(0, ScoreMemberPair(0, 'null'))
    newCursor = resp[0]
    rawResults = resp[1]
    results = []
    Len = len(rawResults)
    for i in range(0, Len, 2):
        field = rawResults[i]
        value = 'null'
        if i < Len - 1:
            value = rawResults[i + 1]
        results.append(ScoreMemberPair(toFloat(value), toStr(field)))
    return ScanResult(newCursor, results)
        
    
