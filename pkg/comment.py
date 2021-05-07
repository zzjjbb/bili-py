from .other import bv2av
import requests
import json


class Comment:
    CANCEL_UPVOTE = 0
    DO_UPVOTE = 1

    def __init__(self, doc_id, **kwargs):
        if doc_id[:2].upper() == 'BV':
            self.type = 1
            self.oid = bv2av(doc_id)
        elif doc_id[:2].upper() == 'AV':
            self.type = 1
            self.oid = int(doc_id[2:])
        else:
            raise ValueError('unknown document type')
        self.cookies = kwargs.pop('cookies', {})
        for i in kwargs:
            setattr(self, i, kwargs[i])

    def basic(self, **kwargs):
        url = "https://api.bilibili.com/x/v2/reply"
        params = {
            'type': self.type,
            'oid':  self.oid
        }
        cookies = self.cookies.copy().update(kwargs.get('cookies', {}))
        response = requests.get(url, params, cookies=cookies)
        data = json.loads(response.content)
        code = data['code']
        msg = data['message']
        if code == 0:
            return data['data']
        else:
            raise Exception(msg)

    def send(self, message):
        url = "http://api.bilibili.com/x/v2/reply/add"
        params = {
            'type':    self.type,
            'oid':     self.oid,
            'message': str(message),
            'csrf':    self.cookies.get('bili_jct', None)
        }
        requests.post(url, params)

    def upvote(self, rpid, action=DO_UPVOTE):
        url = "http://api.bilibili.com/x/v2/reply/action"
        params = {
            'type':   self.type,
            'oid':    self.oid,
            'rpid':   rpid,
            'action': action,
            'csrf':   self.cookies.get('bili_jct', None)
        }
        requests.post(url, params)
