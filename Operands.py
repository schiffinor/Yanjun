"""
Not going to lie pretty shamelessly taken from https://tomerfiliba.com/blog/Infix-Operators
I just wanted to use custom operators in python and this is the best way to do it.
This top class is a basic class that is purely a renamed version of the original class.
The other two classes I defined are left and right operands.
"""

from functools import partial


class InOperand(object):
    def __init__(self, func):
        self.func = func

    def __or__(self, other):
        return self.func(other)

    def __ror__(self, other):
        return InOperand(partial(self.func, other))

    def __call__(self, v1, v2):
        return self.func(v1, v2)


class LeftOperand(object):
    def __init__(self, func):
        self.func = func

    def __or__(self, other):
        return self.func(other)

    def __ror__(self, other):
        raise NotImplementedError("This is a left operand, it cannot be used as a right operand")

    def __call__(self, val):
        return self.func(val)


class RightOperand(object):
    def __init__(self, func):
        self.func = func

    def __or__(self, other):
        raise NotImplementedError("This is a right operand, it cannot be used as a left operand")

    def __ror__(self, other):
        return self.func(other)

    def __call__(self, val):
        return self.func(val)
