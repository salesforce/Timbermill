import os

from timbermill import timberlog
from timbermill.timberlog import timberlog_start


@timberlog_start
def decorator_success():
    timberlog.info(context={'msg': 'this is a successful decorator task'})


@timberlog_start
def decorator_fail():
    timberlog.info(context={'msg': 'this is a unsuccessful decorator task'})
    raise Exception()


def success():
    with timberlog.start_task('context_manager_success') as t:
        t.info(context={'msg': 'this is a successful task'})


def fail():
    with timberlog.start_task('context_manager_fail') as t:
        t.info(context={'msg': 'this is an unsuccessful task'})

        raise Exception()


def log_spot():
    timberlog.spot('example_spot_event')


def log_classic_success():
    try:
        timberlog.start('log_classic_success')
        timberlog.info(context={'msg': 'this is a classic successful log'})
        timberlog.success()
    except:
        timberlog.end_with_error()


def log_classic_error():
    try:
        timberlog.start('log_classic_error')
        timberlog.info(context={'msg': 'this is a classic unsuccessful log'})
        raise Exception()
        timberlog.success()
    except:
        timberlog.end_with_error()


if __name__ == '__main__':
    # timberlog.init(os.getenv('timbermill_server_url'), 'example_env', static_event_params={'a': 1})

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

    log_spot()

    log_classic_success()
    log_classic_error()
