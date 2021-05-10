from copy import deepcopy
from itertools import chain, count
import json
import requests
from ..other.exceptions import APIError

URL_BASE = "https://api.live.bilibili.com/xlive/web-room/v1/"


class Danmaku:
    URL = URL_BASE + "dM/getDMMsgByPlayBackID"

    def __init__(self, rid):
        self.rid = rid

    def __getitem__(self, item):
        params = {
            'rid':   self.rid,
            'index': item
        }
        response = requests.get(self.URL, params)
        data = json.loads(response.content)
        code = data['code']
        msg = data['message']
        if code == 0:
            return data['data']['dm']
        elif code == 10002:
            raise IndexError(msg)
        else:
            raise APIError(code, msg)


class RecList:
    URL = URL_BASE + "record/getList"

    def __init__(self, room_id, page_size=20):
        self.room_id = room_id
        # self._page = 0
        self._page_size = page_size
        self._count = None
        self._cache = {}

    def get_page(self, page, force=False):
        if not self._count:
            force = True
        if not force:
            if self._page_size * (page - 1) >= self._count:
                return []
            if page in self._cache:
                return deepcopy(self._cache[page])

        params = {
            "room_id":   self.room_id,
            "page":      page,
            "page_size": self._page_size
        }
        response = requests.get(self.URL, params)
        data = json.loads(response.content)
        code = data['code']
        msg = data['message']

        if code == 0:
            if force:
                self._count = data['data']['count']
            else:
                assert self._count == data['data']['count']
            self._cache[page] = data['data']['list']
            return deepcopy(data['data']['list'])
        else:
            raise APIError(code, msg)

    def __iter__(self):
        def pages():
            for p in count(1):
                r_list = self.get_page(p)
                if not r_list:
                    break
                yield r_list

        return chain.from_iterable(pages())


class URLList:
    URL = URL_BASE + "record/getLiveRecordUrl"

    def __init__(self, rid, platform='html5'):
        self.rid = rid
        self.platform = platform
        self._urls = None
        self._metadata = None

    def get_data(self, force=False):
        if force or self._urls is None:
            params = {
                'rid':      self.rid,
                'platform': self.platform
            }
            response = requests.get(self.URL, params)
            data = json.loads(response.content)
            code = data['code']
            msg = data['message']
            if code == 0:
                metadata = data['data']
                urls = metadata.pop('list')
                self._urls = urls
                self._metadata = metadata
                return urls, metadata
            else:
                raise APIError(code, msg)

    @property
    def metadata(self):
        self.get_data()
        return deepcopy(self._metadata)

    def __getitem__(self, item):
        self.get_data()
        return self._urls[item]
