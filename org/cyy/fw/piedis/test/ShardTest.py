# -*- coding: GBK -*-
'''
Created on 2015年7月11日

@author: yunyun
'''
import unittest

from org.cyy.fw.piedis.Server import ServerNode, NodeSharder
from org.cyy.fw.piedis.util import Hasher


class ShardTest(unittest.TestCase):
    
    def setUp(self):
        nodes = [];
        node1 = ServerNode();
        node1.setHost("192.168.1.101");
        node1.setPort(6379);
        node2 = ServerNode();
        node2.setHost("192.168.1.102");
        node2.setPort(6379);
        node3 = ServerNode();
        node3.setHost("192.168.1.103");
        node3.setPort(6379);
        node4 = ServerNode();
        node4.setHost("192.168.1.104");
        node4.setPort(6379);
        node5 = ServerNode();
        node5.setHost("192.168.1.105");
        node5.setPort(6379);
        node6 = ServerNode();
        node6.setHost("192.168.1.106");
        node6.setPort(6379);
        node7 = ServerNode();
        node7.setHost("192.168.1.107");
        node7.setPort(6379);
        node8 = ServerNode();
        node8.setHost("192.168.1.108");
        node8.setPort(6379);
        node9 = ServerNode();
        node9.setHost("192.168.1.109");
        node9.setPort(6379);
        node10 = ServerNode();
        node10.setHost("192.168.1.100");
        node10.setPort(6379);
        nodes.append(node1);
        nodes.append(node2);
        nodes.append(node3);
        nodes.append(node4);
        nodes.append(node5);
        nodes.append(node6);
        nodes.append(node7);
        nodes.append(node8);
        nodes.append(node9);
        nodes.append(node10);
        self.sharder = NodeSharder(nodes);
    
    def testMD5(self):
        host = '192.168.1.105'
        print(Hasher.hashByMD5(host))
        
    def test_sharder(self):
        testNumber = 100000;
        result = {};
        for i in range(0, testNumber):
            key = "key-" + str(i);
            node = self.sharder.getShardNodeInfo(key);
            host = node.getHost();
            number = result.get(host);
            if (number == None): 
                number = 0;
            number += 1
            result[host] = number;
        
        for (k, v) in result.items():
            print(str(k) + ":" + str(v));
        
