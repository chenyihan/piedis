import unittest

from org.cyy.fw.piedis.Client import PiedisClient
from org.cyy.fw.piedis.Server import ServerNode


class BaseCmdTest(unittest.TestCase):
    def setUp(self):
        host = "192.168.1.106";
        port = 6379;
        self.client = PiedisClient()
        self.client.setHost(host)
        self.client.setPort(port)
        
    def tearDown(self):
        self.client.close()
