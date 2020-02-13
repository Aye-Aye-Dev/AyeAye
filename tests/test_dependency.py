'''
Created on 13 Feb 2020

@author: si
'''
import unittest

import ayeaye
from ayeaye.dependency import model_connections

class FakeModel(ayeaye.Model):
    insects = ayeaye.Connect(engine_url="fake://bugsDB")


class TestConnect(unittest.TestCase):
    
    def test_get_data_connections(self):
        engine_urls = [c.relayed_kwargs['engine_url'] for c in model_connections(FakeModel)]
        self.assertEqual(['fake://bugsDB'], engine_urls)
