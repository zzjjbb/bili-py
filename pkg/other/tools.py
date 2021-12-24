import requests
from requests import Response
import json
from .exceptions import APIError

def load_data(data: dict, err_handler=None):
    code = data['code']
    msg = data['message']

    if err_handler is not None:
        err_handler(code, msg)

    if code == 0:
        return data['data']
    else:
        raise APIError(code, msg)
