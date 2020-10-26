class RestResponseMock:
    def __init__(self):
        self.ok = True


class RestClientBlackHole:
    def post(self, *args, **kwargs):
        return RestResponseMock()
