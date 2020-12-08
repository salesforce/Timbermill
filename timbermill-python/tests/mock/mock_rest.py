import json
import queue as Queue


class MockRest:

    class MockResult:

        @property
        def ok(self):
            return True

    def __init__(self):
        self.events = dict()

    def post(self, url, data, headers, **kwargs):
        for event_dict in json.loads(data)['events']:
            key = event_dict['@type']
            val = json.dumps(event_dict)

            if key in self.events:
                val_queue = self.events[key]
                val_queue.put(val)
                self.events[key] = val_queue
            else:
                queue = Queue.Queue()
                queue.put(val)
                self.events[key] = queue

        return MockRest.MockResult()

    def get_request(self, event_type):
        val_queue = self.events[event_type]
        return val_queue.get()
