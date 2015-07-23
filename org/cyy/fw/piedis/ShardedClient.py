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
        
    def set(self, key, value):
        client = self.getClient(key)
        return client.set(key, value)    
    
    def getClient(self, key):
#         serverStr = str(serverNode)
        shardNodeInfo = self.nodeSharder.getShardNodeInfo(key)
        client = self.serverClientMapping.get(shardNodeInfo)
        if client != None:
            return client
        client = PiedisClient()
        client.setHost(shardNodeInfo.getHost())
        client.setPort(shardNodeInfo.setPort())
        client.setConnectTimeout(self.connectTimeout)
        self.serverClientMapping[shardNodeInfo] = client
        return client
        
