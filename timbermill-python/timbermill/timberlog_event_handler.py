import json
import logging
import os
import socket
import sys
import threading
import time
from datetime import datetime, timedelta
from queue import Queue

import requests

from timbermill.timberlog_mock import RestClientBlackHole
import timbermill.timberlog_consts as consts

LOG = None

SEND_EVENTS_SIZE_THRESHOLD_IN_MB = 1
SEND_EVENTS_SECONDS_TIMEOUT = int(os.getenv('TIMBERMILL_EVENT_SEND_INTERVAL', 0))  # 0 means sending events synchronously
TIMBERMILL_ENABLED = os.getenv('TIMBERMILL_LOG_ENABLED', 'true').lower() == 'true'
TIMBERMILL_URL = None
ENV = None
STATIC_EVENT_PARAMS = None

rest_client = None

events_queue = None
event_collection_thread = None

submit_event = None
drain_events_from_queue = None


def init(timbermill_hostname: str, env: str = None, logger=None, static_event_params={}):
    global TIMBERMILL_URL
    TIMBERMILL_URL = f'http://{timbermill_hostname}/events'

    global ENV
    ENV = env if env is not None else 'default'

    global LOG
    LOG = logger if logger is not None else logging

    global STATIC_EVENT_PARAMS
    STATIC_EVENT_PARAMS = static_event_params

    init_timbermill()


def init_timbermill():
    global rest_client
    global submit_event
    global drain_events_from_queue
    global events_queue

    LOG.info(f'Initializing timbermill for process {os.getpid()}')

    events_queue = Queue()
    if TIMBERMILL_ENABLED:
        rest_client = requests
    else:
        rest_client = RestClientBlackHole()

    if SEND_EVENTS_SECONDS_TIMEOUT > 0:  # case async
        LOG.info(f'Going to start timbermill in async mode with {SEND_EVENTS_SECONDS_TIMEOUT} seconds send interval')
        __start_event_collection_thread()
        submit_event = __submit_event_async
    else:
        LOG.info(f'Going to start timbermill synchronously')
        submit_event = __submit_event_sync


def create_event(event_type: str, text: dict, name: str = None, task_id: str = None, context: dict = {}, strings: dict = {}, metrics: dict = {}, parent_id: str = None, retention_days: int = None,
                 event_time: str = None, status: bool = None) -> dict:
    if not event_time:
        event_time = __get_current_time_formatted()

    event_strings = dict(strings)  # Copy to new dict
    event_strings['host'] = socket.gethostname()
    event_strings['processId'] = os.getpid()
    event_strings['threadName'] = str(threading.current_thread().ident) + '(' + threading.current_thread().name + ')'

    text = __dict_values_to_str(text)
    context = __dict_values_to_str(context)
    event_strings = __dict_values_to_str(event_strings)

    event = {'@type': event_type, consts.TASK_ID: task_id, 'context': context, 'strings': event_strings, 'metrics': metrics, 'text': text, 'time': event_time, 'env': ENV}
    if __should_add_static_event_params(event_type, name):
        event['strings'] = {**STATIC_EVENT_PARAMS, **event['strings']}

    if name:
        event['name'] = name

    if parent_id:
        event['parentId'] = parent_id

    if retention_days:
        event['dateToDelete'] = __get_current_time_formatted(retention_days)

    if status:
        event['status'] = status

    return event


def __should_add_static_event_params(event_type, name):
    return event_type == consts.EVENT_TYPE_START or \
           (event_type == consts.EVENT_TYPE_SPOT and name != consts.END_WITHOUT_START and name != consts.LOG_WITHOUT_CONTEXT)


def __get_current_time_formatted(plus_days: int = 0) -> str:
    return (datetime.now() + timedelta(days=plus_days)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def __dict_values_to_str(dictionary: dict) -> dict:
    return {k: str(v) for k, v in dictionary.items()}


def __start_event_collection_thread():
    global event_collection_thread
    event_collection_thread = threading.Thread(target=__drain_events_from_queue_async)
    event_collection_thread.setDaemon(True)
    event_collection_thread.start()
    LOG.info('timbermill thread started')


def __drain_events_from_queue_async():
    while True:
        time.sleep(SEND_EVENTS_SECONDS_TIMEOUT)
        events_to_send = []

        while not events_queue.empty():
            event = events_queue.get()
            events_to_send.append(event)
            events_to_send_in_mb = sys.getsizeof(events_to_send) / 1000000
            if events_to_send_in_mb > SEND_EVENTS_SIZE_THRESHOLD_IN_MB:  # send events in chunks
                __submit_events_to_timbermill(events_to_send)
                events_to_send = []

        if events_to_send:  # send the remainder
            __submit_events_to_timbermill(events_to_send)


def __submit_event_async(event: dict):
    events_queue.put(event)


def __submit_event_sync(event: dict):
    __submit_events_to_timbermill([event])


def __submit_events_to_timbermill(events: list):
    global TIMBERMILL_URL

    try:
        events_to_send = {'@type': 'EventsWrapper', 'events': events}
        res = rest_client.post(TIMBERMILL_URL, data=json.dumps(events_to_send), headers={'content-type': 'application/json'}, timeout=2)
        if not res.ok:
            LOG.warning(f'Problem while sending to timbermill: {res.reason}')
    except Exception as e:
        LOG.warning(f'Failed while sending to timbermill, message was: {json.dumps(events_to_send)}', exc_info=True)
