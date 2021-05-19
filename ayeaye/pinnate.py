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
        :param data: dictionary or dictionary encoded in json or instance of Pinnate
        """
        self._attr = {}
        if isinstance(data, self.__class__):
            self._attr = data._attr
        elif data:
            self.load(data, merge=False)

    def __unicode__(self):
        d = ', '.join([u"{}:{}".format(k, v) for k, v in self._attr.items()])
        return '<Pinnate %s>' % d

    def __str__(self):
        return self.__unicode__().encode("ascii", "replace").decode()

    def keys(self):
        return self._attr.keys()

    def values(self):
        return self._attr.values()

    def items(self):
        return self._attr.items()

    def __contains__(self, key):
        return key in self._attr

    def as_dict(self, select_fields=None):
        """
        @param select_fields: (list of str) to only include some fields from model.
        @return: (dict) with mixed values
        """
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
            return {k: v.as_dict() if isinstance(v, self.__class__) else v \
                    for k, v in self._attr.items()}

    def as_json(self, *args, **kwargs):
        """
        @see :method:`as_dict` for params.
        @returns (str) JSON representation
        """
        return json.dumps(self.as_dict(*args, **kwargs), default=str)

    def __getattr__(self, attr):
        if attr not in self._attr:
            raise AttributeError("{} instance has no attribute '{}'".format(self.__class__.__name__, attr))
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
        if attr != '_attr':
            self._attr[attr] = val

    def __getitem__(self, key):
        return self._attr[key]

    def __setitem__(self, key, value):
        self._attr[key] = value

    def get(self, key, default=None):
        return self._attr.get(key, default)

    def load(self, data, merge=False):
        """
        :param data: dict or json string
        :param merge: bool see :method:`update` if False or :method:`merge` when True.
        """

        if not isinstance(data, dict):
            data = json.loads(data)

        if merge:
            self.merge(data)
        else:
            self.update(data)

    def update(self, data):
        """
        Extend the Pinnate with further settings. If a setting with an existing key is supplied,
        then the previous value is overwritten.

        :param data: dictionary or dictionary encoded in json
        """
        for k, v in data.items():
            if isinstance(v, dict):
                self._attr[k] = Pinnate(v)
            else:
                self._attr[k] = v

    def merge(self, data):
        """
        Extend the Pinnate with further settings. If a setting with an existing key is supplied,
        then the previous value is either updated (if the previous value is a dict) or overwritten
        (if the previous value is a scalar).

        Where corresponding values are not of compatible types, a ValueError is raised. Compatible
        means that an existing dict value must remain a dict (thus the dicts are merged), or a
        non-dict value must remain a non-dict type.

        :param data: dictionary or dictionary encoded in json
        """
        for k, v in data.items():
            if isinstance(v, dict):
                try:
                    self._attr[k].merge(v)
                except KeyError:
                    self._attr[k] = Pinnate(v)
                except AttributeError:
                    raise ValueError("Invalid key '{}'".format(k))
            else:
                if k in self._attr and isinstance(self._attr[k], self.__class__):
                    msg = ("Key '{}' attempted to overwrite an existing Pinnate."
                            "Operation not permitted."
                            )
                    raise ValueError(msg.format(k))
                self._attr[k] = v
