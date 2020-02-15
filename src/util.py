from bisect import bisect, bisect_left


class NearestNeighborDict(dict):
    """
    A class used to represent a data structure that for a given input date returns an iterator to the object holding the closest excisting date, in a descending order
    ...

    Attributes
    ----------
    _keylist : list
        a list of sorted object keys
    iter_index : int
        an index that keeps track of current iterator location 
    """
        def __init__(self):
            dict.__init__(self)
            self._keylist = []
            self.iter_index = 0

        def __getitem__(self, x):
            if x in self:
                return dict.__getitem__(self, x)
            index = bisect_left(self._keylist, x)

            if index == 0:
                raise KeyError('No prev date')
            index -= 1
            return dict.__getitem__(self, self._keylist[index])

        def __setitem__(self, key, value):
            if key not in self:
                index = bisect(self._keylist, key)
                self._keylist.insert(index, key)
            dict.__setitem__(self, key, value)

        def __iter__(self):
            while self.iter_index > 0:
                self.iter_index -= 1
                yield self._keylist[self.iter_index]

        def __call__(self, key):
            index = bisect_left(self._keylist, key)
            if index < len(self._keylist) - 1 and self._keylist[index + 1] == key:
                self.iter_index = index + 1
            else:
                self.iter_index = index
            return self



