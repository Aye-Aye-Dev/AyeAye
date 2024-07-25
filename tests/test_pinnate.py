from datetime import datetime
import pickle
import unittest

from ayeaye.pinnate import Pinnate


class TestPinnate(unittest.TestCase):
    def test_attrib_and_dict(self):
        a = Pinnate({"my_string": "abcdef"})
        self.assertEqual(a.my_string, "abcdef")
        self.assertEqual(a["my_string"], "abcdef")
        self.assertEqual(a.as_dict(), {"my_string": "abcdef"})

    def test_recurse(self):
        d = {"my_things": [1, 2, {"three": 3}]}
        a = Pinnate(d)
        p = a.my_things
        self.assertTrue(p[0] == 1 and p[1] == 2 and isinstance(p[2], Pinnate))
        self.assertEqual(p[2].three, 3)

    def test_variable_method_name(self):
        """
        check it's possible to give a variable the same name as an existing method.
        """
        d = {"as_dict": 1}
        p = Pinnate(d)
        self.assertEqual("{'as_dict': 1}", str(p.as_dict()))

    def test_as_json(self):
        d = {
            "number": 1,
            "string": "hello",
            "date": datetime.strptime("2020-01-15 10:34:12", "%Y-%m-%d %H:%M:%S"),
            "recurse_list": [{"abc": "def"}],
            "recurse_dict": {"ghi": {"jkl": "mno"}},
        }
        p = Pinnate(d)
        as_json = str(p.as_json())
        expected = (
            '{"number": 1, "string": "hello", "date": "2020-01-15 10:34:12", '
            '"recurse_list": [{"abc": "def"}], "recurse_dict": {"ghi": {"jkl": "mno"}}}'
        )
        self.assertEqual(expected, as_json)

    def test_recursive_lists(self):
        "bug found with list inside a list"

        d = {
            "name": "Stall Street",
            "type": "Line",
            "payload": [[-2.3597675, 51.3797509], [-2.359739, 51.3797783]],
            "tags": {
                "name": "Stall Street",
                "oneway": "yes",
                "highway": "unclassified",
                "motor_vehicle:conditional": "no @ (10:00-18:00)",
            },
        }
        p = Pinnate(d)
        # accessing the item triggered the bug
        self.assertEqual(-2.3597675, p.payload[0][0])

        # deeper recurse
        d = {"a": [[[{"b": "hello"}]]]}
        p = Pinnate(d)
        self.assertEqual("hello", p.a[0][0][0].b)

    def test_top_level_list(self):
        "test you can make a Pinnate from a top level list"

        l = ["a", "b", "c"]

        p = Pinnate(l)

        self.assertEqual(p[0], "a")
        self.assertEqual(p[1], "b")
        self.assertEqual(p[2], "c")

    def test_top_level_list_of_dict(self):
        "test you can make a Pinnate from a top level list of dictionaries"

        l = [{"first_name": "mary"}, {"last_name": "poppins"}]

        p = Pinnate(l)

        self.assertEqual(p[0].first_name, "mary")
        self.assertEqual(p[1].last_name, "poppins")

    def test_update_dict(self):
        "test you can update an existing dict into a Pinnate made from a dict"

        first_name = {"first_name": "mary"}
        p = Pinnate(first_name)

        last_name = {"last_name": "poppins"}
        p.update(last_name)

        self.assertEqual(p.first_name, "mary")
        self.assertEqual(p.last_name, "poppins")

    def test_update_list(self):
        "test you can update a list into a Pinnate made from a list"

        l_one = [1, 2]
        p = Pinnate(l_one)

        l_two = [2, 3]
        p.update(l_two)

        self.assertEqual(p[0], 1)
        self.assertEqual(p[1], 2)
        self.assertEqual(p[2], 2)
        self.assertEqual(p[3], 3)

    def test_top_level_iterator(self):
        "a list at the top level makes the Pinnate behave like a normal list"

        p = Pinnate(["a", "b", "c"])
        joined_values = "".join(p)

        self.assertEqual("abc", joined_values)
        self.assertFalse(isinstance(p, list), "It's a Pinnate, not a list")

        # same behaviour at 2nd level
        px = Pinnate({"xx": ["a", "b", "c"]})
        joined_values = "".join(px.xx)
        self.assertEqual("abc", joined_values)
        self.assertIsInstance(px.xx, list, "Second level really is a list")

    def test_late_construction(self):
        "Pinnate becomes a list or dict or set but not during construction"
        # list
        p = Pinnate()
        p.append("x")
        self.assertEqual("x", p[0])

        # set
        p = Pinnate()
        p.add("x")
        self.assertIn("x", p)

        # dict
        p = Pinnate()
        p.x = "y"
        self.assertEqual(p["x"], "y")

        # dict with set item
        p = Pinnate()
        p["x"] = "y"
        self.assertEqual(p["x"], "y")

        # nothing there so 'in' must be false
        p = Pinnate()
        self.assertFalse("x" in p)

    def test_set_item_list(self):
        "positional access to a list. .__setitem__ also used with dictionaries"
        p = Pinnate(["a", "b", "c"])
        self.assertEqual("a", p[0])

        p[0] = "x"
        self.assertEqual("x", p[0])

    def test_serialise(self):
        "It should be possible to pickle and un-pickle Pinnate objects."

        # list
        p = Pinnate(["a", "b", "c"])
        pinnate_text = pickle.dumps(p)
        p_hydrated = pickle.loads(pinnate_text)
        self.assertEqual("c", p_hydrated[2])

        more_complex = {"a": {"b": ["c"]}}
        p = Pinnate(more_complex)
        pinnate_text = pickle.dumps(p)
        p_hydrated = pickle.loads(pinnate_text)
        self.assertEqual("c", p_hydrated.a.b[0])

        # payload_undefined
        p = Pinnate(None)
        pinnate_text = pickle.dumps(p)
        p_hydrated = pickle.loads(pinnate_text)
        self.assertEqual(None, p_hydrated.as_native())


if __name__ == "__main__":
    unittest.main()
