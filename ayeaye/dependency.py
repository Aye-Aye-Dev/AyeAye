'''
Find build dependencies and data provenance between ayeaye.Models.

Created on 13 Feb 2020

@author: si
'''
from ayeaye.connect import Connect

def model_connections(model_cls):
    """
    Generator yielding instances of :class:`ayeaye.connect.Connect` from a model.

    @param model_cls: class , not instance. Expected to be subclass of :class:`ayeaye.Model` but
            not checked.
    """
    for obj_name in dir(model_cls):
        obj = getattr(model_cls, obj_name)
        if issubclass(obj.__class__, Connect):
            yield obj
