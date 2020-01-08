import unittest

from ayeaye.pinnate import Pinnate

class TestConnectors(unittest.TestCase):
    def test_attrib_and_dict(self):
        a = Pinnate({'my_string':'abcdef'})
        assert a.my_string == 'abcdef'
        assert a['my_string'] == 'abcdef'
        assert a.as_dict() == {'my_string': 'abcdef'}

    def test_recurse(self):
        d={'my_things' : [1,2,{'three':3}]}
        a = Pinnate(d)
        p = a.my_things
        assert p[0] == 1 and p[1] == 2 and isinstance(p[2], Pinnate)
        assert p[2].three == 3
