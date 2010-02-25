from collections import deque
class SizedDict(dict):
    
    def __init__(self, size=10000):
        dict.__init__(self)
        self._maxsize = size
        self._stack = deque()

    def __setitem__(self, key, value):
        if key not in self:
            self._stack.append(key)
        if len(self._stack) >= self._maxsize:
            self.__delitem__(self._stack.popleft())
        dict.__setitem__(self, key, value)

    def __delitem__(self,key):
        dict.__delitem__(self,key)
