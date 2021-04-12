# -*- coding: UTF-8 -*-
# _author_ = "luoyanpeng"
import collections

class LRUCache(collections.OrderedDict):

    def __init__(self, size=10):
        self.size = size
        self.cache = collections.OrderedDict()

    def clear(self):
        self.cache.clear()


    def setsize(self,size):
        self.size = size
        self.clear()

    def get(self, key):
        if key in self.cache.keys():
            value = self.cache.pop(key)
            self.cache[key] = value
            return value
        else:
            value = None
            return value

    def set(self, key, value):
        if key in self.cache.keys():
            self.cache.pop(key)
            self.cache[key] = value
        elif self.size == len(self.cache):
            self.cache.popitem(last=False)
            self.cache[key] = value
        else:
            self.cache[key] = value

    def isFull(self):
        return self.size == len(self.cache)