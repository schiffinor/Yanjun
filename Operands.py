from functools import partial

import numpy as np


class InOperator(object):
    def __init__(self, func):
        self.func = func

    def __or__(self, other):
        return self.func(other)

    def __ror__(self, other):
        return InOperator(partial(self.func, other))

    def __call__(self, v1, v2):
        return self.func(v1, v2)


class RelOperator(InOperator):
    def __init__(self, func):
        super().__init__(func)

    def __or__(self, other):
        return self.func(other)

    def __ror__(self, other):
        return RelOperator(partial(self.func, other))

    def __call__(self, v1, v2):
        return self.func(v1, v2)


class PreFixOperator(object):
    def __init__(self, func):
        self.func = func

    def __or__(self, other):
        return self.func(other)

    def __ror__(self, other):
        raise NotImplementedError("This is a postfix operator, it cannot be used to the left of the operand.")

    def __call__(self, val):
        return self.func(val)


class PostFixOperator(object):
    def __init__(self, func):
        self.func = func

    def __or__(self, other):
        raise NotImplementedError("This is a prefix operator, it cannot be used as to the right of the operand.")

    def __ror__(self, other):
        return self.func(other)

    def __call__(self, val):
        return self.func(val)


class LAssOperator(InOperator):
    """
    A left-associative operator.
    When used as:  a |Lop| b, the operator's __ror__ returns a new LAssOperator
    with the left operand pre-applied. Then __or__ applies the operator to the right operand.
    This makes chaining like: a |Lop| b |Lop| c work as ((a Lop b) Lop c).
    """

    def __init__(self, func):
        super().__init__(func)

    def __ror__(self, other):
        return LAssOperator(partial(self.func, other))

    def __or__(self, other):
        return self.func(other)  # type: ignore


class RAssOperator(object):
    """
    A right-associative operator.
    When used as: a |rop| b |rop| c, the grouping is a |rop| (b |rop| c).
    Because Python evaluates left-to-right, we delay evaluation by returning an RAssChain.
    The final evaluation is triggered by calling .evaluate() on the chain.
    """

    def __init__(self, func):
        self.func = func

    def __ror__(self, left):
        return RAssChain(self.func, left)

    def __or__(self, right):
        raise NotImplementedError("Right-associative operator must be used between two operands.")


class RAssChain:
    """
    Holds a chain of right-associative operations.
    For an expression like: a |rop| b |rop| c, the chain is built as:
      RAssChain(func, a) __or__(b)  --> becomes a chain node with left=a and right=b.
      Then, when an operator is encountered, it is stored as pending until the next operand arrives.
      Evaluation is done recursively from the right.
    """

    def __init__(self, func, left):
        self.func = func
        self.left = left
        self.right = None  # can be a value or another RAssChain
        self.pending_operator = None  # holds a function if an operator is pending

    def __or__(self, other):
        if isinstance(other, RAssOperator):
            # When an operator appears, store its function as pending.
            if self.right is None:
                raise NotImplementedError("Operator cannot follow without an operand.")
            if self.pending_operator is not None:
                raise NotImplementedError("Two consecutive operators not allowed.")
            self.pending_operator = other.func
            return self
        else:
            # other is an operand
            if self.pending_operator is not None:
                # When there is a pending operator, combine the current right side with the new operand
                # using the pending operator and create a new chain for the right side.
                self.right = RAssChain(self.pending_operator, self.right)
                self.pending_operator = None
                self.right = self.right | other
                return self
            else:
                if self.right is None:
                    self.right = other
                else:
                    if isinstance(self.right, RAssChain):
                        self.right = self.right | other
                    else:
                        self.right = RAssChain(self.func, self.right)
                        self.right = self.right | other
                return self

    def evaluate(self):
        """
        Evaluate the chain right-associatively.
        """
        if isinstance(self.right, RAssChain):
            right_val = self.right.evaluate()
        else:
            right_val = self.right
        return self.func(self.left, right_val)

    def __repr__(self):
        return f"RAssChain({self.left} ? {self.right})"


class FunctionOperator:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __or__(self, other):
        # If 'other' is callable, assume it's another operator and compose.
        # Composition: (f | g)(x) = f(g(x))
        if callable(other):
            # Wrap other if it's not already a FunctionOperator.
            if not isinstance(other, FunctionOperator):
                other = FunctionOperator(other)
            return FunctionOperator(lambda x: self.func(other.func(x)))
        else:
            # If 'other' is not callable, then treat it as an operand.
            return self.func(other)

    def __ror__(self, other):
        # This is invoked when a non-callable appears on the left.
        return self.func(other)


# A decorator to wrap functions as FunctionOperators.
def operator(fn):
    return FunctionOperator(fn)


if __name__ == '__main__':
    # Test InOperator with addition
    add = InOperator(lambda a, b: a + b)
    result_add = 1 | add | 2  # Expected: 3
    print("InOperator add: 1 |add| 2 =", result_add)

    # Test RelOperator with less-than
    less = RelOperator(lambda a, b: a < b)
    result_less1 = 3 | less | 4  # Expected: True
    result_less2 = 4 | less | 3  # Expected: False
    print("RelOperator less: 3 |less| 4 =", result_less1)
    print("RelOperator less: 4 |less| 3 =", result_less2)

    # Test PreFixOperator with negation
    neg = PreFixOperator(lambda a: -a)
    result_neg = neg | 5  # Expected: -5
    print("PreFixOperator neg: neg | 5 =", result_neg)

    # Test PostFixOperator with squaring
    square = PostFixOperator(lambda a: a * a)
    result_square = 5 | square  # Expected: 25
    print("PostFixOperator square: 5 | square =", result_square)

    # Test LAssOperator with subtraction (left-associative)
    expL = LAssOperator(lambda a, b: a ** b)
    result_expL = 2 | expL | 3  # Expected: 7
    result_expL_chain = 2 | expL | 3 | expL | 2  # Expected: 6
    print("LAssOperator expL: 2 |expL| 3 =", result_expL)
    print("LAssOperator expL chaining: 2 |expL| 3 |expL| 2 =", result_expL_chain)

    # Test RAssOperator with exponentiation (right-associative)
    expR = RAssOperator(lambda a, b: a ** b)
    # Single operation: 2 |exp| 3 should be interpreted as 2 ** 3 = 8
    chain1 = 2 | expR | 3
    result_exp1R = chain1.evaluate()  # Expected: 8
    print("RAssOperator expR: 2 |expR| 3 =", result_exp1R)

    # Chained operations: 2 |exp| 3 |exp| 2 should be interpreted as 2 ** (3 ** 2) = 512
    chain2 = 2 | expR | 3 | expR | 2
    result_expR2 = chain2.evaluate()  # Expected: 512
    print("RAssOperator expR chaining: 2 |expR| 3 |expR| 2 =", result_expR2)

    # Another chain: 2 |exp| 2 |exp| 3 should be 2 ** (2 ** 3) = 256
    chain3 = 2 | expR | 2 | expR | 3
    result_expR3 = chain3.evaluate()  # Expected: 256
    print("RAssOperator exp chaining: 2 |expR| 2 |expR| 3 =", result_expR3)

    # Create a sample 2x2 complex matrix.
    M = np.array([[1 + 2j, 3 + 4j],
                  [5 + 6j, 7 + 8j]])


    # Define operators for complex conjugate and transpose.
    # Example operator functions.
    @operator
    def conj(M):
        # For example, if M is a NumPy array, return its complex conjugate.
        # (If not using NumPy, replace with an appropriate implementation.)
        return M.conjugate()


    @operator
    def transpose(M):
        # For example, if M is a NumPy array, return its transpose.
        return M.T


    # Compose the operators. The expression below computes:
    # conj(transpose(M))
    result = conj | transpose | M
    print("Original matrix:\n", M)
    print("Conjugate of the transpose:\n", result)
