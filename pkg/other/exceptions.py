class APIError(Exception):
    def __init__(self, code=None, message=None, *more_messages):
        self.code = code
        self.message = ', '.join([repr(m) for m in [message, *more_messages]])
        super().__init__(f"error code: {code}, message: {self.message}")
