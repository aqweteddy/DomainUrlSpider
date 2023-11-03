import numpy as np
import random

_memomask = {}
def hash_function(n):
    """
    :param n: the index of the hash function
    :return: a generated hash function
    """
    mask = _memomask.get(n)

    if mask is None:
        random.seed(n)
        mask = _memomask[n] = random.getrandbits(32)

    def my_hash(x):
        return hash(str(x) + str(n)) ^ mask

    return my_hash

class CountMinSketch(object):
    """
    A non GPU implementation of the count min sketch algorithm.
    """
    def __init__(self, d=8, w=2 ** 22, M=None):
        self.d = d
        self.w = w
        self.hash_functions = [hash_function(i) for i in range(d)]
        if len( self.hash_functions) != d:
            raise ValueError("The number of hash functions must match match the depth. (%s, %s)" % (d, len(self.hash_functions)))
        if M is None:
            self.M = np.zeros([d, w], dtype=np.int32)
        else:
            self.M = M

    def add(self, x, delta=1):
        for i in range(self.d):
            self.M[i][self.hash_functions[i](x) % self.w] += delta

    def query(self, x):
        return min([self.M[i][self.hash_functions[i](x) % self.w] for i in range(self.d)])

    def get_matrix(self):
        return self.M