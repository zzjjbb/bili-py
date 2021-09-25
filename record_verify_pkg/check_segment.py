import json
from difflib import SequenceMatcher
from collections import namedtuple


class HashCheckDifference(Exception):
    pass


class Segment(namedtuple('Segment',
                         ['keyframe_md5', 'segment_md5', 'start_pts', 'end_pts', 'frames', 'time_base'],
                         defaults=(None,) * 4)):
    start: float
    end: float
    start_str: str
    end_str: str

    _mm_ss_format = staticmethod(lambda x: "{:>02d}:{:>06.3f}".format(int(x) // 60, x % 60))

    def __getattr__(self, item):
        if item in ['start', 'end']:
            for check_field in [item + '_pts', 'time_base']:
                if getattr(self, check_field) is None:
                    raise AttributeError(f"cannot get attribute '{item}' with unknown '{check_field}'")
            return getattr(self, item + '_pts') * self.time_base['video']
        elif item in ['start_str', 'end_str']:
            return self._mm_ss_format(getattr(self, item[:-4]))
        raise AttributeError(f"'{type(self).__name__}' object has no attribute {item}")


class Part(list):
    def __init__(self, info):
        super().__init__(Segment(time_base=info['time_base'], **seg) for seg in info['data'])
        self.name = info['name']
        self.time_base = info['time_base']


class SubPart:
    def __init__(self, part: Part, start=None, end=None):
        self.part = part
        self.start = start
        self.end = end

    name = property(lambda self: self.part.name)
    time_base = property(lambda self: self.part.time_base)

    @property
    def time_range(self):
        return self[0].start, self[-1].end

    @property
    def duration(self):
        return (self[0].start_pts * self[-1].end_pts) * self.part.time_base['video']

    def __len__(self):
        return len(range(len(self.part))[self.start:self.end])

    def __getitem__(self, item):
        return self.part[self.start:self.end][item]

    def __repr__(self):
        data = {'name':       self.name,
                'time_range': (self[0].start_str, self[-1].end_str),
                'slice':      slice(self.start, self.end)}
        return f"{type(self).__name__}({data.__repr__()[1:-1]})"


class ConnectedPart(list):
    pass


def check_subpart(sp_list):
    from collections import Counter
    err_array = bytearray(len(sp_list))
    seg_num = len(sp_list[0])
    for seg_i in range(seg_num):
        for t in ['video', 'audio']:
            tally = Counter([part_i[seg_i].segment_md5[t] for part_i in sp_list]).most_common()
            if len(tally) > 1:
                if seg_i == seg_num - 1:
                    pass  # todo: mark as error
                else:
                    raise HashCheckDifference(
                        f"{t} check error at {sp_list[0][seg_i].start_str}"
                    )
    # max(enumerate(sp_list), key=lambda x:x[1])
    # return sp_list[0]


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
