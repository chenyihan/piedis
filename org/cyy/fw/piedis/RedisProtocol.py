# -*- coding: GBK -*-
'''
Created on 2015年7月8日

@author: yunyun
'''

import sys

from org.cyy.fw.piedis.util import  Decoder
from org.cyy.fw.piedis.util.Exception import PiedisException, ProtocolException


DOLLAR_BYTE = b'$'
ASTERISK_BYTE = b'*'
PLUS_BYTE = b'+'
MINUS_BYTE = b'-'
COLON_BYTE = b':'
NULL = "null";
SIZETABLE = (9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, sys.maxsize)
DIGIT_TENS = (b'0', b'0', b'0', b'0', b'0', b'0',
        b'0', b'0', b'0', b'0', b'1', b'1', b'1', b'1', b'1', b'1', b'1', b'1', b'1',
        b'1', b'2', b'2', b'2', b'2', b'2', b'2', b'2', b'2', b'2', b'2', b'3', b'3',
        b'3', b'3', b'3', b'3', b'3', b'3', b'3', b'3', b'4', b'4', b'4', b'4', b'4',
        b'4', b'4', b'4', b'4', b'4', b'5', b'5', b'5', b'5', b'5', b'5', b'5', b'5',
        b'5', b'5', b'6', b'6', b'6', b'6', b'6', b'6', b'6', b'6', b'6', b'6', b'7',
        b'7', b'7', b'7', b'7', b'7', b'7', b'7', b'7', b'7', b'8', b'8', b'8', b'8',
        b'8', b'8', b'8', b'8', b'8', b'8', b'9', b'9', b'9', b'9', b'9', b'9', b'9',
        b'9', b'9', b'9')
DIGIT_ONES = (b'0', b'1', b'2', b'3', b'4', b'5',
        b'6', b'7', b'8', b'9', b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8',
        b'9', b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'0', b'1',
        b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'0', b'1', b'2', b'3', b'4',
        b'5', b'6', b'7', b'8', b'9', b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7',
        b'8', b'9', b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'0',
        b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'0', b'1', b'2', b'3',
        b'4', b'5', b'6', b'7', b'8', b'9', b'0', b'1', b'2', b'3', b'4', b'5', b'6',
        b'7', b'8', b'9',)

DIGITS = (b'0', b'1', b'2', b'3', b'4', b'5', b'6',
        b'7', b'8', b'9', b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h', b'i', b'j',
        b'k', b'l', b'm', b'n', b'o', b'p', b'q', b'r', b's', b't', b'u', b'v', b'w',
        b'x', b'y', b'z')

def writeIntCrLf(value, data):
    if value < 0:
        data += b'-'
        value = -value
    size = 0
    while(value > SIZETABLE[size]):
        size += 1
    size += 1
    q, r = 0, 0
    num = b''
    while(value >= 65536):
        q = value // 100
        r = value - ((q << 6) + (q << 5) + (q << 2))
        value = q
#         data += DIGIT_ONES[r]
#         data += DIGIT_TENS[r]
        num = DIGIT_ONES[r] + num
        num = DIGIT_TENS[r] + num
    while(True):
        q = (value * 52429) >> (16 + 3)
        r = value - ((q << 3) + (q << 1))
#         data += DIGITS[r]
        num = DIGITS[r] + num
        value = q;
        if value == 0:
            break;
    
    data += num
    return writeCrLf(data)
    
def writeCrLf(data):
    data += b'\r'
    data += b'\n'
    return data

def isNull(text):
    return text == NULL

def parseResponse(sock):
    b = sock.recv(1)
    if b == MINUS_BYTE:
        parseError(sock)
    elif b == ASTERISK_BYTE:
        return parseMultiBulkReply(sock)
    elif b == COLON_BYTE:
        return  parseInteger(sock)
    elif b == DOLLAR_BYTE:
        return parseBulkReply(sock)
    elif b == PLUS_BYTE:
        return parseStatusCodeReply(sock)
    else:
        raise ProtocolException("Unknown reply: " + Decoder.decodeData(b))

def parseInteger(sock):
    return int(readLine(sock))

def parseError(sock):
    message = readLine(sock)
    raise ProtocolException(message)

def parseBulkReply(sock):
    length = int(readLine(sock))
    if(length == -1):
        return NULL.encode()
    readBytes = sock.recv(length)
    sock.recv(1)
    sock.recv(1)
    return readBytes

def parseStatusCodeReply(sock):
    return readLine(sock)

def parseMultiBulkReply(sock):
    num = int(readLine(sock))
    if(num == -1):
        return []
    ret = []
    for _ in range(0, num):
        try:
            ret.append(parseResponse(sock))
        except:
            pass
    return ret

def readLine(sock):
    sb = ""
    while(True):
        b = sock.recv(1)
        if(b == b'\r'):
            c = sock.recv(1)
            if(c == b'\n'):
                break;
            sb += Decoder.decodeData(b)
            sb += Decoder.decodeData(c)
        else:
            sb += Decoder.decodeData(b)
    
    if(len(sb) == 0):
        raise PiedisException('It seems like server has closed the connection.')
    return sb


def generateRequestData(binaryCommand):
    command = binaryCommand.getCommand()
    args = binaryCommand.getArgs()
    data = b''
    data += ASTERISK_BYTE
    argsLen = len(args) + 1
    data = writeIntCrLf(argsLen, data)
    data += DOLLAR_BYTE
    data = writeIntCrLf(len(command), data)
    data += command
    data = writeCrLf(data)
    for arg in args:
        data += DOLLAR_BYTE
        data = writeIntCrLf(len(arg), data)
        data += arg
        data = writeCrLf(data)
    return data
