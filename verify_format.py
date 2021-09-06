from timeline_check import strong_test, weak_test, TimelineFormatError
import os

ASDB_DIR = './db/2021/06/timeline'
for i in os.listdir(ASDB_DIR):
    with open(os.path.join(ASDB_DIR, i), encoding='utf8') as f:
        try:
            print(strong_test(f.read()))
        except TimelineFormatError as e:
            print(i, e)
