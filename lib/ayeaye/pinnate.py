import json


class Pinnate:
    """
    Dictionary or attribute access to variables loaded either from a JSON
    string or supplied as a dictionary.

    >>> a = Pinnate({'my_string':'abcdef'})
    >>> a.my_string
    'abcdef'
    >>> a['my_string']
    'abcdef'
    >>> a.as_dict()
    {'my_string': 'abcdef'}

    objects within lists-

    >>> from ayeaye.pinnate import Pinnate
    >>> d={'my_things' : [1,2,{'three':3}]}
    >>> a = Pinnate(d)
    >>> a.my_things
    [1, 2, <ayeaye.pinnate.Pinnate object at 0x108526e10>]
    >>> a.my_things[2].three
    3
    """

    def __init__(self, data=None):
        """
        :param data: mixed
            can be dictionary or list or set or dictionary/list encoded in json or instance of Pinnate
        """
        # this is the 'payload', it's type is decided on first use. It's typically a dictionary because
        # the attribute nature of Pinnate is the most useful feature. It can also be a list or set.
        self._attr = None

        if isinstance(data, self.__class__):
            self._attr = data._attr
        elif data:
            self.load(data)

    @property
    def payload_undefined(self):
        """
        No data has been set

        @return: boolean
            No data has been provided so _attrib's type hasn't yet been determined
        """
        return self._attr is None

    def is_payload(self, *payload_type):
        """
        :class:`Pinnate` can hold mixed data types. Inspect current payload type.

        e.g.
        >>> p = Pinnate({1:2})
        >>> p.is_payload(dict)
        True

        @param *payload_type: type
            e.g. dict, set or list
            when multiple payload types are given just one has to match

        @return: boolean
        """
        return any([type(self._attr) == pt for pt in payload_type])

    def __unicode__(self):
        as_str = str(self._attr)
        return f"<Pinnate {as_str}>"

    def __str__(self):
        return self.__unicode__().encode("ascii", "replace").decode()

    def keys(self):
        if self.payload_undefined or not self.is_payload(dict):
            raise TypeError("Payload data isn't a dictionary")

        return self._attr.keys()

    def values(self):
        if self.payload_undefined:
            raise TypeError("Payload data hasn't been set")

        if self.is_payload(dict):
            return self._attr.values()

        if self.is_payload(list, set):
            return self._attr

        raise TypeError("Unknown payload data type")

    def items(self):
        if self.payload_undefined or not self.is_payload(dict):
            raise TypeError("Payload data isn't a dictionary")

        return self._attr.items()

    def __contains__(self, key):
        if self.payload_undefined:
            raise TypeError("Payload data hasn't been set")

        if self.is_payload(dict, set):
            return key in self._attr

        raise TypeError("Operation not possible with current payload data type")

    def __iter__(self):
        """
        return a generator

        For lists and sets the generator yields each item. For dictionaries it yield (key, value)
        """
        if self.is_payload(set, list):
            return iter(self._attr)

        if self.is_payload(dict):
            as_key_pairs = [(k, v) for k, v in self._attr.items()]
            return iter(as_key_pairs)

    def as_dict(self, select_fields=None):
        """
        @param select_fields: (list of str) to only include some fields from model.
        @return: (dict) with mixed values
        """
        if not self.is_payload(dict):
            raise TypeError(f"as_dict() can only be called when the payload data is a dictionary")

        if select_fields is not None:
            r = {}
            for k in select_fields:
                if isinstance(self._attr[k], self.__class__):
                    v = self._attr[k].as_dict()
                else:
                    v = self._attr[k]
                r[k] = v
            return r
        else:
            return {
                k: v.as_native() if isinstance(v, self.__class__) else v
                for k, v in self._attr.items()
            }

    def as_native(self):
        """
        @return: (mixed)
            representation of the payload (as children elements) comprised of
            native python data types.
        """
        if self.payload_undefined:
            return None

        if self.is_payload(dict):
            return self.as_dict()

        if self.is_payload(list):
            r = []
            for item in self._attr:
                value = item.as_native() if isinstance(item, self.__class__) else item
                r.append(value)
            return r

        if self.is_payload(set):
            r = set()
            for item in self._attr:
                value = item.as_native() if isinstance(item, self.__class__) else item
                r.add(value)
            return r

        raise TypeError("Unsupported type")

    def as_json(self, *args, **kwargs):
        """
        @see :method:`as_dict` for params.
        @returns (str) JSON representation
        """
        return json.dumps(self.as_native(*args, **kwargs), default=str)

    def __getattr__(self, attr):
        if attr not in self._attr:
            raise AttributeError(
                "{} instance has no attribute '{}'".format(self.__class__.__name__, attr)
            )
        if isinstance(self._attr[attr], list):

            def list_recurse(item):
                r = []
                for s in item:
                    if isinstance(s, dict):
                        r.append(self.__class__(s))
                    elif isinstance(s, list):
                        r.append(list_recurse(s))
                    else:
                        r.append(s)
                return r

            return list_recurse(self._attr[attr])

        elif isinstance(self._attr[attr], dict):
            return self.__class__(self._attr[attr])
        else:
            return self._attr[attr]

    def __setattr__(self, attr, val):
        super(Pinnate, self).__setattr__(attr, val)
        if attr != "_attr":
            if self.payload_undefined:
                self._attr = {}

            self._attr[attr] = val

    def __getitem__(self, key):
        return self._attr[key]

    def __setitem__(self, key, value):
        self._attr[key] = value

    def get(self, key, default=None):
        return self._attr.get(key, default)

    def load(self, data):
        """
        :param data: dict, list, set or json string
        :param merge: bool see :method:`update` if False or :method:`merge` when True.
        """

        if isinstance(data, str):
            data = json.loads(data)

        self.update(data)

    def update(self, data):
        """
        Extend the Pinnate with further payload values.

        If a setting with an existing key is supplied, then the previous value is overwritten.

        If the payload is a -
         - list - it will be extended
         - set - added to
         - dict - merged

        :param data: dictionary or list or set or json string
        """

        if not isinstance(data, (dict, list, set)):
            raise TypeError("Unsupported type")

        if self.payload_undefined:

            if isinstance(data, dict):
                self._attr = {}
            elif isinstance(data, set):
                self._attr = set()
            elif isinstance(data, list):
                self._attr = []

        if not self.is_payload(type(data)):
            p_type = str(type(self._attr))
            d_type = str(type(data))
            msg = (
                f"The type of the update data '{d_type}' doesn't match current payload's "
                f"type: '{p_type}'"
            )
            raise TypeError(msg)

        if self.is_payload(dict):
            for k, v in data.items():
                if isinstance(v, dict):
                    self._attr[k] = Pinnate(v)
                else:
                    self._attr[k] = v

        elif self.is_payload(list):

            for v in data:
                if isinstance(v, dict):
                    self._attr.append(Pinnate(v))
                else:
                    self._attr.append(v)

        elif self.is_payload(set):

            for v in data:
                if isinstance(v, dict):
                    self._attr.add(Pinnate(v))
                else:
                    self._attr.add(v)

    def append(self, item):
        """
        Can be used to add item when the payload is a list.
        """
        self.update([item])

    def add(self, item):
        """
        Can be used to add item when the payload is a list.
        """
        self.update(set([item]))
