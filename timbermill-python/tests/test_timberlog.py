import json
import unittest
from datetime import datetime

from tests.mock.mock_rest import MockRest
from timbermill import timberlog_event_handler, timberlog
from timbermill import timberlog_consts as consts
from timbermill.timberlog import timberlog_start


class TestTimberlog(unittest.TestCase):
    def setUp(self):
        timberlog.init('test')
        mock_rest = MockRest()
        timberlog_event_handler.rest_client = mock_rest
        timberlog_event_handler.ENABLE_PYTHORAMA_TIMBERMILL_2 = True

    def test_start_task(self):
        timberlog.start("test_start_task")
        timberlog.add_context(attr1='attr1', attr2='attr2')
        timberlog.success()

        start_task = self.check_event_type(consts.EVENT_TYPE_START)
        info_task = self.check_event_type(consts.EVENT_TYPE_INFO)
        success_task = self.check_event_type(consts.EVENT_TYPE_END_SUCCESS)
        start_task_id, __ = self.get_task_id(start_task)
        info_task_id, __ = self.get_task_id(info_task)
        success_task_id, __ = self.get_task_id(success_task)
        self.assertEqual(start_task_id, info_task_id)
        self.assertEqual(start_task_id, success_task_id)

    def test_spot_task(self):
        timberlog.spot("test_spot", context={'status': 'OK'}, metrics={'test_num': 2, 'key': 102})
        spot_task = self.check_event_type(consts.EVENT_TYPE_SPOT)
        task_id, parent_id = self.get_task_id(spot_task)
        self.assertEqual(consts.EVENT_TYPE_SPOT, spot_task['@type'])

    def test_with_statement(self):
        with timberlog.start_task("task_with_statement") as timberlogContext:
            timberlogContext.info(context={'status': 'OK'})
        self.check_event_type(consts.EVENT_TYPE_START)
        self.check_event_type(consts.EVENT_TYPE_INFO)
        self.check_event_type(consts.EVENT_TYPE_END_SUCCESS)

    def test_end_with_error(self):
        self.assertRaises(Exception, self.end_with_error_func)
        self.check_event_type(consts.EVENT_TYPE_START)
        self.check_event_type(consts.EVENT_TYPE_INFO)
        self.check_event_type(consts.EVENT_TYPE_INFO)
        self.check_event_type(consts.EVENT_TYPE_END_ERROR)

    def test_info_without_start_task(self):
        timberlog.add_context(attr1='attr1', attr2='attr2')
        event_task = self.check_event_type(consts.EVENT_TYPE_SPOT)
        self.assertEqual(event_task['name'], consts.LOG_WITHOUT_CONTEXT)

    def test_end_without_start_task(self):
        timberlog.success()
        event_task = self.check_event_type(consts.EVENT_TYPE_SPOT)
        self.assertEqual(event_task['name'], consts.END_WITHOUT_START)

    def test_inner_task(self):
        with timberlog.start_task("first_task") as first_timberlog_context:
            first_timberlog_context.info(context={'attr1': 'attr1'})
            with timberlog.start_task("second_task") as second_timberlog_context:
                second_timberlog_context.info(context={'attr2': 'attr2'})

        first_start_task = self.check_event_type(consts.EVENT_TYPE_START)
        self.check_event_type(consts.EVENT_TYPE_INFO)
        second_start_task = self.check_event_type(consts.EVENT_TYPE_START)
        self.check_event_type(consts.EVENT_TYPE_INFO)
        self.check_event_type(consts.EVENT_TYPE_END_SUCCESS)
        first_success_task = self.check_event_type(consts.EVENT_TYPE_END_SUCCESS)

        first_task_id, first_parent_id = self.get_task_id(first_start_task)
        second_task_id, second_parent_id = self.get_task_id(second_start_task)
        self.assertEqual(first_task_id, second_parent_id)
        first_end_task_id, __ = self.get_task_id(first_success_task)
        self.assertEqual(first_task_id, first_end_task_id)

    @timberlog_start
    def test_decorator(self):
        tl.info(context={'attr1': 'attr1'})

    def test_datetime_format(self):
        current_time = timberlog_event_handler.get_current_time_formatted()
        plus_days = 7
        current_time_plus_7_days = timberlog_event_handler.get_current_time_formatted(plus_days)

        datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        try:
            datetime.strptime(current_time, datetime_format)
        except:
            self.fail('Current datetime format is invalid..')
        try:
            datetime.strptime(current_time_plus_7_days, datetime_format)
        except:
            self.fail('Plus days datetime format is invalid..')

    def check_event_type(self, event_type):
        event_task_json = timberlog_event_handler.rest_client.get_request(event_type)
        event_task = json.loads(event_task_json)
        assert event_task['@type'] == event_type

        return event_task

    def end_with_error_func(self):
        with timberlog.start_task("end_with_error_task") as timberlogContext:
            timberlogContext.info(context={'status': 'OK'})
            timberlogContext.info(metrics={'test_num': 4, 'key': 104})
            raise Exception('test exception')

    def get_task_id(self, event_task):
        parent_id = None
        task_id = event_task[consts.TASK_ID]
        if 'parentId' in event_task:
            parent_id = event_task['parentId']

        return task_id, parent_id
