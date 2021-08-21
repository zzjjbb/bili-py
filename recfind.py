import requests
import json
from datetime import datetime
from pkg.other import bv2av, av2bv

with open("note.json") as f:
    note = json.load(f)

asmid = [note[n]['mid'] for n in ['av', 'be', 'ca', 'di', 'ei', 'as']]

for i in range(508440, 600000, 1):
    data = requests.get("https://api.live.bilibili.com/xlive/web-room/v1/record/getInfoByLiveRecord",
                        {"rid": "R" + av2bv(i)[2:]}).json()['data']
    if data is None:
        print(i, ": None")
    else:
        print(i, ': ', datetime.fromtimestamp(data['live_record_info']['start_timestamp']))
        print(data['live_record_info'])
        if (mid := int(data['live_record_info']['uid'])) in asmid:
            for n in ['av', 'be', 'ca', 'di', 'ei', 'as']:
                if note[n]['mid'] == mid:
                    print(n)
            print(data['dm_info'])
            break
