from datetime import datetime
import unittest

from ayeaye.pinnate import Pinnate


class TestPinnate(unittest.TestCase):
    def test_attrib_and_dict(self):
        a = Pinnate({'my_string': 'abcdef'})
        self.assertEqual(a.my_string, 'abcdef')
        self.assertEqual(a['my_string'], 'abcdef')
        self.assertEqual(a.as_dict(), {'my_string': 'abcdef'})

    def test_recurse(self):
        d = {'my_things': [1, 2, {'three': 3}]}
        a = Pinnate(d)
        p = a.my_things
        self.assertTrue(p[0] == 1 and p[1] == 2 and isinstance(p[2], Pinnate))
        self.assertEqual(p[2].three, 3)

    def test_variable_method_name(self):
        """
        check it's possible to give a variable the same name as an existing method.
        """
        d = {'as_dict': 1}
        p = Pinnate(d)
        self.assertEqual("{'as_dict': 1}", str(p.as_dict()))

    def test_as_json(self):
        d = {'number': 1,
             'string': 'hello',
             'date': datetime.strptime("2020-01-15 10:34:12", "%Y-%m-%d %H:%M:%S"),
             'recurse_list': [{'abc': 'def'}],
             'recurse_dict': {'ghi': {'jkl': 'mno'}
                              },
             }
        p = Pinnate(d)
        as_json = str(p.as_json())
        expected = ('{"number": 1, "string": "hello", "date": "2020-01-15 10:34:12", '
                    '"recurse_list": [{"abc": "def"}], "recurse_dict": {"ghi": {"jkl": "mno"}}}'
                    )
        self.assertEqual(expected, as_json)

    def test_recursive_lists(self):
        "bug found with list inside a list"

        d = {'name': 'Stall Street',
             'type': 'Line',
             'payload': [[-2.3597675, 51.3797509], [-2.359739, 51.3797783]],
             'tags': {'name': 'Stall Street',
                      'oneway': 'yes',
                      'highway': 'unclassified',
                      'motor_vehicle:conditional': 'no @ (10:00-18:00)'
                      }
             }
        p = Pinnate(d)
        # accessing the item triggered the bug
        self.assertEqual(-2.3597675, p.payload[0][0])


        # deeper recurse
        d = {'a': [[[{'b':'hello'}]]]}
        p = Pinnate(d)
        self.assertEqual('hello', p.a[0][0][0].b)
