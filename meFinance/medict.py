class SizedDict(dict):
    ''' Sized dictionary without timeout. '''

    def __init__(self, size=10000):
        dict.__init__(self)
        self._maxsize = size
        self._stack = []

    def __setitem__(self, key, value):
        if key not in self:
            self._stack.append(key)
        if len(self._stack) >= self._maxsize:
            for i in range(int((self._maxsize)/4)):
                self.__delitem__(self._stack[0])
        dict.__setitem__(self, key, value)

    def __delitem__(self,key):
        self._stack.remove(key)
        dict.__delitem__(self,key)

    def get(self, key):  # Not used.  Too damn slow.
        if key in self:
            self._stack.remove(key)
            self._stack.append(key)
        return self.__getitem__(key)
