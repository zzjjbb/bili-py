from copy import deepcopy
from itertools import chain, count
import json
import requests
from ..other import tools

URL_BASE = "https://api.live.bilibili.com/xlive/web-room/v1/"


class Danmaku:
    URL = URL_BASE + "dM/getDMMsgByPlayBackID"
    EAGER_LOAD = 10

    def __init__(self, rec):
        if isinstance(rec, RecInfo):
            self.rec_info = rec
            self.rid = rec.rid
        else:
            self.rec_info = None
            self.rid = rec

    def get(self, workers=1):
        if workers != 1:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(workers) as e:
                if self.rec_info is None:
                    self.rec_info = RecInfo(self.rid)
                if not self.rec_info.initialized():
                    get_info = e.submit(lambda: self.rec_info.raw_data)
                    tasks = [e.submit(self.get_segment, i) for i in range(self.EAGER_LOAD)]
                    dm_num = get_info.result()['dm_info']['num']
                    if dm_num <= self.EAGER_LOAD:
                        tasks = tasks[:dm_num]
                    else:
                        tasks.extend([
                            e.submit(self.get_segment, i)
                            for i in range(self.EAGER_LOAD, dm_num)
                        ])
                else:
                    tasks = [e.submit(self.get_segment, i) for i in range(self.rec_info.dm_info['num'])]

            all_dm = [i.result() for i in tasks]
            dm_ = []
            in_ = []
            for i in all_dm:
                dm_.extend(i['dm_info'])
                in_.extend(i['interactive_info'])
            return {'dm_info': dm_, 'interactive_info': in_}

    def get_segment(self, item):
        params = {
            'rid':   self.rid,
            'index': item
        }
        response = requests.get(self.URL, params)
        data = tools.load_data(response.json())
        return data['dm']


class RecInfo:
    URL = URL_BASE + "record/getInfoByLiveRecord"
    room_id: int
    uid: int
    title: str
    area_id: int
    parent_area_id: int
    area_name: str
    parent_area_name: str
    start_timestamp: int
    end_timestamp: int
    online: int
    dm_info: dict

    def __init__(self, rid):
        self.rid = rid
        self._raw_data = None

    def __getattr__(self, item):
        if item == 'dm_info':
            return self.raw_data[item]
        else:
            # elif item in ['room_id', 'uid', 'title', 'area_id', 'parent_area_id', 'area_name', 'parent_area_name',
            #               'start_timestamp', 'end_timestamp', 'online', 'danmu_num', 'live_screen_type']:
            try:
                return self.raw_data['live_record_info'][item]
            except KeyError:
                raise AttributeError(f"{type(self)} object has no attribute '{item}'") from None

    @property
    def raw_data(self):
        if self._raw_data is None:
            params = {"rid": self.rid}
            response = requests.get(self.URL, params)
            data = tools.load_data(response.json())
            self._raw_data = data
        return self._raw_data

    def initialized(self):
        return self._raw_data is not None


class RecList:
    URL = URL_BASE + "record/getList"

    def __init__(self, room_id, page_size=20):
        self.room_id = room_id
        self._page_size = page_size
        self._count = None
        self._cache = {}

    def flush(self):
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
        data = tools.load_data(response.json())
        # force refresh count
        if force:
            self._count = data['count']
        else:
            assert self._count == data['count']
        self._cache[page] = data['list']
        return deepcopy(data['list'])

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
            metadata = tools.load_data(response.json())
            urls = metadata.pop('list')
            self._urls = urls
            self._metadata = metadata
            return urls, metadata

    @property
    def metadata(self):
        self.get_data()
        return deepcopy(self._metadata)

    def __getitem__(self, item):
        self.get_data()
        return self._urls[item]
