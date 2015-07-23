# -*- coding: GBK -*-
'''
Created on 2015年7月9日

@author: yunyun
'''
from org.cyy.fw.piedis.util.Hasher import HashRing

class NodeInfo:
    def __init__(self):
        self.weight = 1
        
    def setName(self, name):
        self.name = name
    
    def getName(self):
        return self.name
    
    def setWeight(self, weight):
        self.weight = weight
        
    def getWeight(self):
        if(self.weight == 0):
            self.weight = 1
        return self.weights
    

class ServerNode(NodeInfo):
    def __init__(self):
        pass
    
    def setHost(self, host):
        self.host = host
    
    def getHost(self):
        return self.host
    
    def setPort(self, port):
        self.port = port
    
    def getPort(self):
        return self.port
    
    def __str__(self):
        return self.host + ":" + str(self.port);

class NodeSharder(NodeInfo):
    def __init__(self, shards):
        self.nodesCache = HashRing(shards)
        
    def getShardNodeInfo(self, key):
        return self.nodesCache.get_node(key)
        
