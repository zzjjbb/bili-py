import json
import sys
from datetime import datetime
from itertools import zip_longest
from pkg.live.record import RecList, Danmaku, URLList
import warnings

PAGE_SIZE = 5
UTC_OFFSET = 8 * 3600


def confirm(s):
    return input(s + " (y/N)").lower() == 'y'


def abort():
    print("Aborted.")
    sys.exit(0)


def select(pages):
    p = 0
    item = None
    while not item:
        print('\n'.join([
            f"[{datetime.utcfromtimestamp(i['start_timestamp'] + UTC_OFFSET).strftime('%Y/%m/%d')}] "
            f"{i['rid']}  {i['title']}" for i in pages[p] if i]))
        c = input(f"Page {p + 1}/{len(pages)}  Select item/Next/Prev/Quit (1-{PAGE_SIZE}/n/p/r/Q)").lower()
        try:
            num = int(c)
            item = pages[p][num - 1]
            if not item:
                print("Invalid index!")
        except IndexError:
            print("Invalid index!")
        except ValueError:
            if c == 'n':
                p = min(p + 1, len(pages) - 1)
            elif c == 'p':
                p = max(p - 1, 0)
            elif c == 'r':
                return None
            else:
                abort()
    return item


if __name__ == '__main__':
    name = input("A-SOUL name:")
    print("Loading record list...")

    with open("note.json") as f:
        note = json.load(f)
    rec_list = RecList(note[name]['roomid'])
    while True:
        # Param of `select` is pages (example p_size=5): [(rec0-rec4), (rec5-rec9),... (..., None)]
        rec = select(list(zip_longest(*([iter(rec_list)] * PAGE_SIZE), fillvalue=None)))
        if rec is None:  # Refresh list
            rec_list.flush()
            continue
        title, rid = rec['title'], rec['rid']
        if confirm(f"RID: {rid}, Title: {title}"):
            break
    if confirm("Download danmaku?"):
        dm = []
        for i, new_dm in enumerate(Danmaku(rid)):
            if isinstance(new_dm_list := new_dm['dm_info'], list):
                dm.extend(new_dm_list)
            else:
                warnings.warn("Invalid danmaku chunk!")
            print(f'Finish getting index {i}, current length {len(dm)}')
        print(f'Reach the end')
        with open(rid + '.json', 'w') as f:
            json.dump(dm, f)
    if confirm("Download with aria2?"):
        import aria2p
        import re

        conf = note['aria2']
        aria2 = aria2p.API(aria2p.Client(**conf['client']))
        all_uri = [u['url'] for u in URLList(rid)]
        date = datetime.utcfromtimestamp(rec['start_timestamp'] + UTC_OFFSET).strftime('%y%m%d')
        options = {'dir': f"{conf['dir']}/[{date}] {title} - {note[name]['name']}/source"}
        for u in all_uri:
            options['out'] = re.search(r".{13}:\d\d:\d\d\.flv", u).group().replace(':', '')
            aria2.add_uris([u], options)
