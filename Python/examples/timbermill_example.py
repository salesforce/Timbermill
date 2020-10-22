from Python.timbermill import timberlog
from Python.timbermill.timberlog import timberlog_start


@timberlog_start
def decorator_success():
    pass


@timberlog_start
def decorator_fail():
    raise Exception()


def success():
    with timberlog.start_task("task_with_statement"):
        pass


def fail():
    with timberlog.start_task("task_with_statement"):
        raise Exception()


if __name__ == '__main__':
    timberlog.init('stg-timbermill2-server.default')

    try:
        decorator_success()
        decorator_fail()

        success()
        fail()
    except Exception:
        pass
