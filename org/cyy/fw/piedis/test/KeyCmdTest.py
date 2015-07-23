# -*- coding: GBK -*-
'''
Created on 2015年7月10日

@author: yunyun
'''
from org.cyy.fw.piedis.Client import PiedisClient
from org.cyy.fw.piedis.Command import ScoreMemberPair, ScanParams, SortingParams
from org.cyy.fw.piedis.test import BaseCmdTest


class KeyCmdTest(BaseCmdTest):
    def test_dump(self):
        self.client.flushAll()
        
        resp = self.client.dump("key1")
        self.assertEqual(resp, b'null')
        
        self.client.set("key1", "value1")
        
        resp = self.client.dump("key1")
        self.assertTrue(resp != b'null');
    
    def test_restore(self):
        self.client.flushAll()
        
        self.client.set('key1', 'value1')
        resp = self.client.dump('key1')
        
        resp = self.client.restore('key2', resp, 0, False)
        self.assertEqual(resp, 'OK')
        
    def test_expire(self):
        self.client.flushAll()
        seconds = 10
        resp = self.client.expire('key1', seconds)
        self.assertTrue(not resp)
        
        self.client.set('key1', 'value1')
        resp = self.client.expire('key1', seconds)
        self.assertTrue(resp)
        
        resp = self.client.ttl('key1')
        self.assertTrue(resp <= seconds)
        
    def test_expireAt(self):
        self.client.flushAll()
        ts = 2355292000
        resp = self.client.expireAt('key1', ts)
        self.assertTrue(not resp)
        
        self.client.set('key1', 'value1')
        resp = self.client.expireAt('key1', ts)
        self.assertTrue(resp)
        
        resp = self.client.ttl('key1')
        self.assertTrue(resp <= ts)
        
    def test_pexpire(self):
        self.client.flushAll()
        millSeconds = 10000
        resp = self.client.pExpire('key1', millSeconds)
        self.assertTrue(not resp)
        
        self.client.set('key1', 'value1')
        resp = self.client.pExpire('key1', millSeconds)
        self.assertTrue(resp)
        
        resp = self.client.pTTL('key1')
        self.assertTrue(resp <= millSeconds)
    
    def test_PExpireAt(self):
        self.client.flushAll()
        ts = 2355292000000
        resp = self.client.pExpireAt('key1', ts)
        self.assertTrue(not resp)
        
        self.client.set('key1', 'value1')
        resp = self.client.pExpireAt('key1', ts);
        self.assertTrue(resp)
        
        resp = self.client.pTTL('key1')
        self.assertTrue(resp <= ts)
        
    def test_object(self):
        self.client.flushAll()
        
        self.client.set('key1', 'value1')
        resp = self.client.objectRefcount('key1')
        
        self.assertEqual(resp, 1)
        resp = self.client.objectIdletime('key1')
        print('idle time:', resp)
        
        self.client.get('key1')
        resp = self.client.objectIdletime('key1')
        print('idle time:', resp)
        
        resp = self.client.objectEncoding('key1')
        self.assertEqual(resp, 'raw')
        
        self.client.set('key1', 158201332432423)
        resp = self.client.objectEncoding('key1')
        self.assertEqual(resp, 'raw')
        
        self.client.set('key1', 20)
        resp = self.client.objectEncoding('key1')
        self.assertEqual(resp, 'int')
    
    def test_persist(self):
        self.client.flushAll()
        
        self.client.set('key1', 'value1')
        self.client.expire('key1', 10)
        
        resp = self.client.ttl('key1')
        self.assertTrue(0 < resp <= 10)
        
        resp = self.client.persist('key1')
        self.assertTrue(resp)
        
        resp = self.client.ttl('key1')
        self.assertEqual(resp, -1)
        
    def test_randomKey(self):
        self.client.flushAll()
        
        resp = self.client.randomKey()
        self.assertEqual(resp, 'null')
        
        self.client.set('key1', 'value1')
        self.client.set('key2', 'value2')
        
        resp = self.client.randomKey()
        self.assertTrue(resp != 'null')
        self.assertTrue(resp == 'key1' or resp == 'key2')
        
    def test_rename(self):
        self.client.flushAll()
        
        try:
            self.client.rename('key1', 'key2')
        except Exception as e:
            print(e)
        
        self.client.set('key1', 'value1')
        resp = self.client.rename('key1', 'key2')
        self.assertEqual(resp, 'OK')
        
        resp = self.client.exists('key1')
        self.assertEqual(resp, False)
        
        resp = self.client.exists('key2')
        self.assertEqual(resp, True)
        
        self.client.set('key3', 'value3')
        self.client.set('key4', 'value4')
        
        resp = self.client.rename('key3', 'key4')
        self.assertEqual(resp, 'OK')
        
        resp = self.client.get('key3')
        self.assertEqual(resp, 'null')
        
        resp = self.client.get('key4')
        self.assertEqual(resp, 'value3')
        
    def test_renameNX(self):
        self.client.flushAll()
        
        try:
            self.client.renameNX('key1', 'key2')
        except Exception as e:
            print(e)
            
        self.client.set('key1', 'value1')
        self.client.set('key2', 'value2')
        
        resp = self.client.renameNX('key1', 'key2')
        self.assertTrue(not resp)
        
        resp = self.client.renameNX('key1', 'key3')
        self.assertTrue(resp)
        
    def test_migrate(self):
        self.client.flushAll()
        destHost = '192.168.1.105'
        destPort = 6379
        
        destClient = PiedisClient().setHost(destHost).setPort(destPort)
        destClient.flushAll()
        
        self.client.set('key1', 'value1')
        
        self.client.migrate('key1', destHost, destPort, 0, 10)
        resp = self.client.exists('key1')
        self.assertTrue(not resp)
        
        resp = destClient.get('key1')
        self.assertEqual(resp, 'value1')
        
    def test_type(self):
        self.client.flushAll()
        self.client.set('key1', 'value1')
        
        resp = self.client.type('key1')
        self.assertEquals(resp, 'string')
        
        self.client.lPush('llkey1', 'llvalue1');
        resp = self.client.type('llkey1')
        self.assertEquals(resp, 'list')
        
        self.client.sAdd('skey1', 'svalue1')
        resp = self.client.type('skey1')
        self.assertEquals(resp, 'set')
        
        scoreMember = ScoreMemberPair(1, 'zvalue1')
        self.client.zAdd('zkey1', scoreMember);
        
        resp = self.client.type('zkey1')
        self.assertEquals(resp, 'zset')
        
        self.client.hSet('hkey1', 'hfield1', 'hvalue1')
        resp = self.client.type('hkey1')
        self.assertEquals(resp, 'hash')
        
    def test_move(self):
        self.client.flushAll()
        
        self.client.set('key1', 'value1')
        
        resp = self.client.move('key1', 1)
        self.assertTrue(resp)
        
        resp = self.client.get('key1')
        self.assertEquals(resp, 'null')
        
        self.client.select(1)
        
        resp = self.client.get('key1')
        self.assertEquals(resp, 'value1')
        
    def test_scan(self):
        self.client.flushAll()
        self.client.set('key1', 'value1')
        self.client.set('key2', 'value2')
        self.client.set('key3', 'value3')
        self.client.set('key4', 'value4')
        self.client.set('key5', 'value5')
        self.client.set('key6', 'value6')
        self.client.set('key7', 'value7')
        self.client.set('key8', 'value8')
        self.client.set('key9', 'value9')
        self.client.set('key10', 'value10')
        self.client.set('key11', 'value11')
        self.client.set('key12', 'value12')
        self.client.set('key13', 'value13')
        self.client.set('key14', 'value14')
        self.client.set('key15', 'value15')
        self.client.set('key16', 'value16')
        self.client.set('key17', 'value17')
        self.client.set('key18', 'value18')
        self.client.set('key19', 'value19')
        self.client.set('key20', 'value20')
        
        count = 5
        cursor = 0
        resp = self.client.scan(cursor, ScanParams().count(count))
        cursor = resp.getCursor()
        results = resp.getResults()
        print('cursor:', cursor)
        print('results:', results)
        
        resp = self.client.scan(cursor, ScanParams().count(count))
        cursor = resp.getCursor()
        results = resp.getResults()
        print('cursor:', cursor)
        print('results:', results)
        
        resp = self.client.scan(cursor, ScanParams().count(count))
        cursor = resp.getCursor()
        results = resp.getResults()
        print('cursor:', cursor)
        print('results:', results)
    
    def test_sort(self):
        self.client.flushAll()
        
        resp = self.client.lPush('key1', '25', '3', '5', '4', '55', '34', '15', '2')
        print(resp)
        resp = self.client.sort('key1', None, None)
        print(resp)
        
        resp = self.client.sort('key1', SortingParams().desc(), None)
        print(resp)
        
        resp = self.client.sort('key1', SortingParams().alpha(), None)
        print(resp)
        
        resp = self.client.sort('key1', SortingParams().limit(2, 4), None)
        print(resp)
        
        self.client.flushAll()
        self.client.lPush('key1', '25', '3', '5', '4', '55', '34', '15', '2')
        
        resp = self.client.sort('key1', None, 'key2')
        print(resp)
