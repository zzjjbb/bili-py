#!/bin/env python3
import json
from xml.etree.ElementTree import ElementTree
import os
import logging
import argparse

logging.basicConfig(format='%(asctime)s [%(levelname).1s] %(message)s', level=logging.INFO)

parser = argparse.ArgumentParser(description="Convert json/xml danmaku file to DPlayer compatible form [v220901]")
parser.add_argument('dir', help="video directory")
cli_args = parser.parse_args()
base_dir = cli_args.dir

src = os.path.join(base_dir, 'source')
dm_list = [i for i in os.listdir(src) if i.split('.')[-1] in ['json', 'xml']]
if len(dm_list) != 1:
    logging.error(f"expecting 1 JSON/XML danmaku file but get {len(dm_list)}")
src = os.path.join(src, dm_list[0])

os.makedirs(os.path.join(base_dir, 'transcoded'), exist_ok=True)
dst = os.path.join(base_dir, 'transcoded', 'danmaku.json')

offset = 0
try:
    with open(os.path.join(base_dir, 'play'), 'r', encoding='utf8') as info_f:
        info = json.load(info_f)
        offset = info['offset']['danmaku']
        logging.info("loaded from 'play' file: offset %+.1fs", offset)
except (FileNotFoundError, KeyError):
    pass


with open(src, 'r', encoding='utf8') as f_in, open(dst, 'w', encoding='utf8') as f_out:
    src_ext = src.split('.')[-1]
    if src_ext == 'json':
        d_in = json.load(f_in)
        d_out = [[
            round(i['ts'] / 1000 + offset, 1),  # time ms->s, 1 digit
            {1: 0, 4: 2, 5: 1}.get(i['dm_mode'], 0),  # position
            i['dm_color'],  # color
            0,  # user_hash, drop it
            i['text']  # content
        ] for i in d_in]
    elif src_ext == 'xml':
        d_out = [[
            round(float((dm_attr := el.get('p').split(','))[0]) + offset, 1),
            {1: 0, 4: 2, 5: 1}.get(int(dm_attr[1]), 0),
            int(dm_attr[3]),  # color
            0,  # user_hash, drop it
            el.text
        ] for el in ElementTree(file=f_in).iter('d')]
    else:
        logging.error("unknown danmaku file type '%s'", src_ext)
    json.dump({'code': 0, 'data': d_out}, f_out, separators=(',', ':'), ensure_ascii=False)
logging.info("finished processing '%s'", src)
