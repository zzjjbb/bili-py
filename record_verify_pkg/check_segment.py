import json
from difflib import SequenceMatcher
from collections import namedtuple


class Segment(namedtuple('Segment', ['start_pts', 'end_pts', 'frames', 'keyframe_md5', 'segment_md5'])):
    pass


class Part(list):
    def __init__(self, info):
        super().__init__(Segment(**seg) for seg in info['data'])
        self.name = info['name']
        self.time_base = info['time_base']


class SubPart:
    def __init__(self, part: Part, start=None, end=None):
        self.part = part
        self.start = start
        self.end = end

    @property
    def name(self):
        return self.part.name

    @property
    def time_range(self):
        tb = self.part.time_base['video']
        mm_ss = lambda x: "{:>02d}:{:>06.3f}".format(int(x)//60, x%60)
        start = mm_ss(self[0].start_pts * tb)
        end = mm_ss(self[-1].end_pts * tb)
        return {'start': start, 'end': end}

    def __getitem__(self, item):
        return self.part[self.start:self.end][item]

    def __repr__(self):
        data = {'name': self.name, 'time': self.time_range, 'slice': slice(self.start, self.end)}
        return f"{type(self).__name__}({data.__repr__()[1:-1]})"


class ConnectedPart(list):
    pass


def best_segment(seg_list):
    return seg_list[0]


def convert_time(seqs, segment_range):
    for long_seg in segment_range:
        pass


def check_single(hash_seqs, key):
    def insert(item, pos):
        nonlocal all_segments
        if pos == len(all_segments):
            all_segments.append([item])
        else:
            all_segments[pos].append(item)

    # init 'previous' information
    seq2 = key(hash_seqs[0])
    matcher = SequenceMatcher()

    # all_segments: each element is a segment with a unique keyframe (assumption)
    all_segments = []
    start = 0
    for seg_i, seg in enumerate(hash_seqs[0]):
        insert({'part': 0, 'seg': seg_i}, seg_i)
    for i in range(1, len(hash_seqs)):
        seq1, seq2 = seq2, key(hash_seqs[i])
        matcher.set_seqs(seq1, seq2)
        blocks = matcher.get_matching_blocks()
        block = blocks[0]
        if len(blocks) == 2:
            if block.b != 0 or block.a + block.size != len(seq1):
                raise ValueError(f"unable to connect part {i} with {i + 1}")
            start += block.a
        elif len(blocks) == 1:
            start += block.a
        else:
            raise ValueError(f"multiple matching blocks in part {i} and {i + 1}")
        for seg_i in range(len(seq2)):
            insert({'part': i, 'seg': seg_i}, seg_i + start)

    segment_range = []
    part_set_prev = {}
    for i in range(len(all_segments)):
        part_set = {part['part'] for part in all_segments[i]}
        if part_set != part_set_prev:
            if i > 0:
                for part_i, part in enumerate(all_segments[i - 1]):
                    prev_range = segment_range[-1][-1]
                    prev_range[part_i].end = part['seg'] + 1
            part_info = [SubPart(hash_seqs[part['part']], part['seg']) for part in all_segments[i]]
            if part_set.isdisjoint(part_set_prev):  # if cannot connect, add a new list
                segment_range.append([part_info])
            else:
                segment_range[-1].append(part_info)
        part_set_prev = part_set
    for part_i, part in enumerate(all_segments[-1]):
        prev_range = segment_range[-1][-1]
        prev_range[part_i].end = part['seg'] + 1
    print(segment_range)

    return segment_range


with open('./checksum.json') as f:
    s1 = json.load(f)
# with open('nfg.json') as f:
#     s2 = json.load(f)


connect = check_single([Part(p) for p in s1], lambda x: [seg.keyframe_md5 for seg in x])

# for part1 in s1:
#     matcher.set_seq1([seg['keyframe_md5'] for seg in part1['data']])
#     for part2 in s2:
#         matcher.set_seq2([seg['keyframe_md5'] for seg in part2['data']])
#         matching_blocks = matcher.get_matching_blocks()
#         print(matching_blocks)

# for block in matching_blocks[:-1]:
#     print('start')
#     print(part1['data'][block.a]['start_pts'] * part1['time_base']['video'])
#     print(part2['data'][block.b]['start_pts'] * part2['time_base']['video'])
#     print('end')
#     print(part1['data'][block.a + block.size - 1]['start_pts'] * part1['time_base']['video'])
#     print(part2['data'][block.b + block.size - 1]['start_pts'] * part2['time_base']['video'])
