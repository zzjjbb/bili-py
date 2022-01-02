#!/bin/env python3
import json
from xml.etree.ElementTree import ElementTree
import sys
import os

src = os.path.join(sys.argv[1], 'source')
dm_list = [i for i in os.listdir(src) if i.split('.')[-1] in ['json', 'xml']]
if len(dm_list) != 1:
    raise ValueError(f"Expect 1 JSON/XML danmaku file but get {len(dm_list)}")
src = os.path.join(src, dm_list[0])

os.makedirs(os.path.join(sys.argv[1], 'transcoded'), exist_ok=True)
dst = os.path.join(sys.argv[1], 'transcoded', 'danmaku.json')

with open(src, 'r', encoding='utf8') as f_in, open(dst, 'w', encoding='utf8') as f_out:
    src_ext = src.split('.')[-1]
    if src_ext == 'json':
        d_in = json.load(f_in)
        d_out = [[
            i['ts'] // 100 / 10,  # time ms->s, 1 digit
            {1: 0, 4: 2, 5: 1}.get(i['dm_mode'], 0),  # position
            i['dm_color'],  # color
            0,  # user_hash, drop it
            i['text']  # content
        ] for i in d_in]
    elif src_ext == 'xml':
        d_out = [[
            round(float((dm_attr := el.get('p').split(','))[0]), 1),
            {1: 0, 4: 2, 5: 1}.get(int(dm_attr[1]), 0),
            int(dm_attr[3]),  # color
            0,  # user_hash, drop it
            el.text
        ] for el in ElementTree(file=f_in).iter('d')]
    else:
        raise NotImplementedError(f"unknown danmaku file type '{src_ext}'")
    json.dump({'code': 0, 'data': d_out}, f_out, separators=(',', ':'), ensure_ascii=False)
