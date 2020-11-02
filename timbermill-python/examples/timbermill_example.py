import os

from timbermill import timberlog
from timbermill.timberlog import timberlog_start


@timberlog_start
def decorator_success():
    pass


@timberlog_start
def decorator_fail():
    raise Exception()


def success():
    with timberlog.start_task('context_manager_success'):
        s = 'this is a successful task'


def fail():
    with timberlog.start_task('context_manager_fail'):
        raise Exception()


if __name__ == '__main__':
    timberlog.init(os.getenv('timbermill_server_url'), 'example_env', static_event_params={'a': 1})

    decorator_success()
    try:
        decorator_fail()
    except Exception:
        pass

    success()
    try:
        fail()
    except Exception:
        pass
