import functools
import inspect
import pathlib
from pprint import pprint
from typing import Dict


_flows = {}

_repository_root = pathlib.Path(__file__).parent.parent.parent.parent


class Flow():
    expectations: Dict[str, 'Expectation']

    def __init__(self, func):
        self.func = func
        self.name = func.__name__
        self.expectations = {}

        file = inspect.getsourcefile(func)
        (lines, lineno) = inspect.getsourcelines(func)

        # Find line where function is defined
        actuallineno = lineno
        for line in lines:
            if line.strip().startswith('def ' + self.name):
                break
            actuallineno += 1

        self.file = file
        self.relpath = pathlib.Path(self.file).relative_to(_repository_root)
        self.lineno = actuallineno


    def __repr__(self):
        return f'Flow({self.name} ({self.file}:{self.lineno})))'


class Expectation():
    def __init__(self, name, expr, typ):
        self.name = name
        self.expr = expr
        self.typ = typ


def _get_or_create_flow(func) -> Flow:
    flow = Flow(func)
    if flow.name not in _flows:
        _flows[flow.name] = flow
    return _flows[flow.name]


def table(_func = None, *, name=None, expr=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        _get_or_create_flow(func)
        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)


def view(_func = None, *, name=None, expr=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        _get_or_create_flow(func)
        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)


def expect_or_drop(name, expr):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        exp = Expectation(name, expr, typ='expect_or_drop')
        _get_or_create_flow(func).expectations[exp.name] = exp
        return wrapper
    return decorator


def expect_or_fail(name, expr):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        exp = Expectation(name, expr, typ='expect_or_fail')
        _get_or_create_flow(func).expectations[exp.name] = exp
        return wrapper
    return decorator


def expect(name, expr):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        exp = Expectation(name, expr, typ='expect')
        _get_or_create_flow(func).expectations[exp.name] = exp
        return wrapper
    return decorator


def print_source_map():
    pprint(_flows)


def get_flow(name) -> Flow:
    return _flows[name]
