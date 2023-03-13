# Created by Saravanan Krishnarajan at 13-03-2023 using PyCharm

# Python memory profile using memory_profiler

import numpy as np
from memory_profiler import profile

@profile
def example_ones():
    d = np.ones((100,1000,1000))
    return d

@profile
def example_two():
    d = np.ones((100,1000,1000))
    s = sum(d)
    return s

if __name__ == '__main__':
    example_ones()
    example_two()

