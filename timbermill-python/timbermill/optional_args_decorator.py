from functools import wraps


def optional_args_decorator(func):
    @wraps(func)
    def wrapped_decorator(*args):
        if len(args) == 1 and callable(args[0]):
            return func(args[0])
        else:
            def real_decorator(decoratee):
                return func(decoratee, *args)

            return real_decorator

    return wrapped_decorator
