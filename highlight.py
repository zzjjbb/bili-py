import os
import json
import sys
import logging
from difflib import SequenceMatcher
import re


def read_highlight(timeline, offset=0):
    timeline = re.split(r'(\d\d:\d\d)', timeline)
    highlight = []
    for i in range(1, len(timeline), 2):
        time, text = timeline[i:i + 2]
        time = int(time[:2]) * 60 + int(time[3:]) + offset
        text = text.strip()
        highlight.append({'time': time, 'text': text})
    return highlight


logging.basicConfig(format='%(asctime)s [%(levelname).1s] %(message)s', level=logging.DEBUG)

MONTH = '2105'
BASE_DIR = 'E:/OneDrive - mail.ustc.edu.cn'
REC_DIR = os.path.join(BASE_DIR, 'A-SOUL_records')
ASDB_DIR = os.path.join(BASE_DIR, 'asdb/db/')

selected_rec = [r for r in os.listdir(REC_DIR) if r[1:5] == MONTH]
selected_db = os.path.join(ASDB_DIR, '20' + MONTH[:2], MONTH[2:])

with open(os.path.join(selected_db, 'main.json'), encoding='utf8') as fp:
    db_info = json.load(fp)

# db_info_visited = [False] * len(db_info)

for rec in selected_rec:
    play_file = os.path.join(REC_DIR, rec, 'play')
    if os.path.isfile(play_file):
        # Search
        db_info_rec = [r for r in db_info if r['date'] == f"{int(rec[3:5])}-{int(rec[5:7])}"]
        if db_info_rec:
            match_size, _, best_match = max(
                [(SequenceMatcher(None, rec.replace('‛', ''), ri['title']).find_longest_match().size, i, ri)
                 for i, ri in enumerate(db_info_rec)])
            if match_size < 6:
                logging.warning(f"{match_size = } < 6, Rec: {rec[9:]}, db: {best_match['title']}")
        else:
            logging.warning("No info in asdb: " + rec)
            continue
        if os.path.isfile(timeline_name := os.path.join(selected_db, 'timeline', best_match['bv'] + '.txt')):
            with open(timeline_name, encoding='utf8') as fp:
                timeline = fp.read()
            with open(play_file, 'r+', encoding='utf8') as fp:
                play_info = json.load(fp)
                fp.seek(0)
                play_info['highlight'] = read_highlight(timeline)
                if not play_info.get('extra'):
                    play_info['extra'] = {}
                play_info['extra']['asdb'] = best_match
                json.dump(play_info, fp, ensure_ascii=False, indent=2)
                fp.truncate()
        else:
            logging.warning('Find info in asdb but no timeline: ' + rec)

    else:
        logging.warning("'play' file not found: " + rec)
