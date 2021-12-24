import json
from difflib import SequenceMatcher
from collections import namedtuple
from itertools import chain
import logging


class HashCheckDifference(Exception):
    pass

class VATuple(namedtuple('VATuple', ['video', 'audio'])):
    pass

class Segment(namedtuple('Segment',
                         ['keyframe_md5', 'segment_md5', 'start_pts', 'end_pts', 'frames', 'time_base'],
                         defaults=(None,) * 6)):
    start: float
    end: float
    start_str: str
    end_str: str

    _mm_ss_format = staticmethod(lambda x: "{:>02d}:{:>06.3f}".format(int(x) // 60, x % 60))

    def __new__(cls, **kwargs):
        if kwargs.get('segment_md5') is not None:
            kwargs['segment_md5'] = VATuple(**kwargs['segment_md5'])
        if kwargs.get('frames') is not None:
            kwargs['frames'] = VATuple(**kwargs['frames'])
        if kwargs.get('time_base') is not None:
            kwargs['time_base'] = VATuple(**kwargs['time_base'])
        return super().__new__(cls, **kwargs)

    def __getattr__(self, item):
        if item in ['start', 'end']:
            for check_field in [item + '_pts', 'time_base']:
                if getattr(self, check_field) is None:
                    raise AttributeError(f"cannot get attribute '{item}' with unknown '{check_field}'")
            return getattr(self, item + '_pts') * self.time_base.video
        elif item in ['start_str', 'end_str']:
            return self._mm_ss_format(getattr(self, item[:-4]))
        raise AttributeError(f"'{type(self).__name__}' object has no attribute {item}")

    def __lt__(self, other):
        if isinstance(other, Segment):
            return self.frames < other.frames
        else:
            raise TypeError("'<' not supported between instances of "
                            f"'{type(self).__name__}' and '{type(other).__name__}'")

    def __eq__(self, other):
        if not isinstance(other, Segment):
            return False
        s = self.segment_md5
        o = other.segment_md5
        if s is None or o is None:
            return s is None and o is None
        return self.segment_md5 == other.segment_md5

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.segment_md5)


class Part(list):
    def __init__(self, info):
        super().__init__(Segment(time_base=info['time_base'], **seg) for seg in info['data'])
        self.name = info['name']
        self.time_base = VATuple(**info['time_base'])

    def __eq__(self, other):
        if self is other:
            return True
        eq = isinstance(other, Part) and self.name == other.name and self.time_base == other.time_base
        return eq and super().__eq__(self, other)

    def __ne__(self, other):
        return not self.__eq__(other)


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
        return (self[0].start_pts * self[-1].end_pts) * self.part.time_base.video

    def __len__(self):
        return len(range(len(self.part))[self.start:self.end])

    def __getitem__(self, item):
        return self.part[self.start:self.end][item]

    def __repr__(self):
        data = {'name':       self.name,
                'time_range': (self[0].start_str, self[-1].end_str),
                'slice':      slice(self.start, self.end)}
        return f"{type(self).__name__}({data.__repr__()[1:-1]})"

    def __eq__(self, other):
        eq = isinstance(other, SubPart) and self.part == other.part
        return eq and self.start == other.start and self.end == other.end


class ConnectPart:
    def __init__(self, all_parts, name=None, key=None):
        self.time_base = self._get_timebase(all_parts)
        self.name = name
        if key is None:
            key = lambda x: [seg_.keyframe_md5 for seg_ in x]
        self._all_segments = []
        self._all_parts = all_parts
        start = 0
        matcher = SequenceMatcher(autojunk=False)
        seq2 = key(all_parts[0])
        for seg_i, seg in enumerate(all_parts[0]):
            self._insert({'part': 0, 'seg': seg_i}, seg_i)
        for i in range(1, len(all_parts)):
            seq1, seq2 = seq2, key(all_parts[i])
            matcher.set_seqs(seq1, seq2)
            blocks = matcher.get_matching_blocks()
            block = blocks[0]
            if len(blocks) == 2:
                if not (block.b == 0 and (block.a + block.size == len(seq1) or block.size == len(seq2))):
                    raise ValueError(f"unable to connect part {i} with {i + 1}")
                start += block.a
            elif len(blocks) == 1:
                start += block.a
            else:
                raise ValueError(f"multiple matching blocks in part {i} and {i + 1}")
            for seg_i in range(len(seq2)):
                self._insert({'part': i, 'seg': seg_i}, seg_i + start)
        # self.all_subparts = self._make_subparts()
        self.subparts = []
        self._make_subparts()
        self.segments = []
        for conn_part in self.subparts:
            self.segments += chain.from_iterable(conn_part)
            self.segments.append(Segment())

    @staticmethod
    def _get_timebase(parts):
        timebase = None
        for p in parts:
            if timebase is None:
                timebase = p.time_base
            else:
                if timebase != p.time_base:
                    raise ValueError("time bases of all Parts are different")
        return timebase

    def _insert(self, item, pos):
        if pos == len(self._all_segments):
            self._all_segments.append([item])
        else:
            self._all_segments[pos].append(item)

    def _make_subparts(self):
        all_subparts = []
        part_set_prev = {}
        all_segments = self._all_segments
        for i in range(len(all_segments)):
            part_set = {part['part'] for part in all_segments[i]}
            if part_set != part_set_prev:
                if i > 0:
                    for part_i, part in enumerate(all_segments[i - 1]):
                        prev_range = all_subparts[-1][-1]
                        prev_range[part_i].end = part['seg'] + 1
                part_info = [SubPart(self._all_parts[seg_i['part']], seg_i['seg']) for seg_i in all_segments[i]]
                if part_set.isdisjoint(part_set_prev):  # if cannot connect, add a new list
                    all_subparts.append([part_info])
                else:
                    all_subparts[-1].append(part_info)
            part_set_prev = part_set
        for part_i, part in enumerate(all_segments[-1]):
            prev_range = all_subparts[-1][-1]
            prev_range[part_i].end = part['seg'] + 1

        for conn_part in all_subparts:
            best_sp = []
            for subparts in conn_part:
                best_sp_i = self._select_best(subparts)
                if best_sp and best_sp[-1].part == best_sp_i.part and best_sp[-1].end == best_sp_i.start:
                    best_sp[-1].end = best_sp_i.end
                else:
                    best_sp.append(best_sp_i)
            self.subparts.append(best_sp)

    @staticmethod
    def _select_best(subparts):
        if len(subparts) == 1:
            return subparts[0]
        best_idx = set(range(len(subparts)))  # Assume all is good at first
        corrupted = False
        for sp in zip(*subparts):
            sp_enum = list(enumerate(sp))
            sp_enum.sort(key=lambda x: (x[1], -x[0]), reverse=True)
            best_idx_i = set()
            for sp_i in sp_enum:
                if sp_i[1] < sp_enum[0][1]:
                    break
                best_idx_i.add(sp_i[0])
                if sp_i[1] != sp_enum[0][1]:
                    corrupted = True
            best_idx &= best_idx_i
        if corrupted:
            logging.warning("Selected SubPart may be corrupted")
        if len(best_idx) > 0:
            return subparts[min(best_idx)]
        else:
            logging.warning("Cannot detect best SubPart. Use the first one as fallback.")
            return subparts[0]

    def __repr__(self):
        return (f"ConnectPart(subparts={self.subparts.__repr__()}, "
                f"segments=[{len(self.segments)} elements])")


def check_subpart(sp_list):
    from collections import Counter
    err_array = bytearray(len(sp_list))
    seg_num = len(sp_list[0])
    for seg_i in range(seg_num):
        for t in ['video', 'audio']:
            tally = Counter([part_i[seg_i].segment_md5._as_dict[t] for part_i in sp_list]).most_common()
            if len(tally) > 1:
                if seg_i == seg_num - 1:
                    pass  # todo: mark as error
                else:
                    raise HashCheckDifference(
                        f"{t} check error at {sp_list[0][seg_i].start_str}"
                    )
    # max(enumerate(sp_list), key=lambda x:x[1])
    # return sp_list[0]


# def check_single(hash_seqs, key):
#     def insert(item, pos):
#         nonlocal all_segments
#         if pos == len(all_segments):
#             all_segments.append([item])
#         else:
#             all_segments[pos].append(item)
#
#     # init 'previous' information
#     seq2 = key(hash_seqs[0])
#     matcher = SequenceMatcher()
#
#     # all_segments: each element is a segment with a unique keyframe (assumption)
#     all_segments = []
#     start = 0
#     for seg_i, seg in enumerate(hash_seqs[0]):
#         insert({'part': 0, 'seg': seg_i}, seg_i)
#     for i in range(1, len(hash_seqs)):
#         seq1, seq2 = seq2, key(hash_seqs[i])
#         matcher.set_seqs(seq1, seq2)
#         blocks = matcher.get_matching_blocks()
#         block = blocks[0]
#         if len(blocks) == 2:
#             if block.b != 0 or block.a + block.size != len(seq1):
#                 raise ValueError(f"unable to connect part {i} with {i + 1}")
#             start += block.a
#         elif len(blocks) == 1:
#             start += block.a
#         else:
#             raise ValueError(f"multiple matching blocks in part {i} and {i + 1}")
#         for seg_i in range(len(seq2)):
#             insert({'part': i, 'seg': seg_i}, seg_i + start)
#
#     segment_range = []
#     part_set_prev = {}
#     for i in range(len(all_segments)):
#         part_set = {part['part'] for part in all_segments[i]}
#         if part_set != part_set_prev:
#             if i > 0:
#                 for part_i, part in enumerate(all_segments[i - 1]):
#                     prev_range = segment_range[-1][-1]
#                     prev_range[part_i].end = part['seg'] + 1
#             part_info = [SubPart(hash_seqs[part['part']], part['seg']) for part in all_segments[i]]
#             if part_set.isdisjoint(part_set_prev):  # if cannot connect, add a new list
#                 segment_range.append([part_info])
#             else:
#                 segment_range[-1].append(part_info)
#         part_set_prev = part_set
#     for part_i, part in enumerate(all_segments[-1]):
#         prev_range = segment_range[-1][-1]
#         prev_range[part_i].end = part['seg'] + 1
#     print(segment_range)
#     return segment_range


with open('s1.json') as f:
    s1 = json.load(f)
with open('nfg.json') as f:
    s2 = json.load(f)
with open('s0.json') as f:
    s3 = json.load(f)

connect1 = ConnectPart([Part(p) for p in s1])
connect2 = ConnectPart([Part(p) for p in s2])
connect3 = ConnectPart([Part(p) for p in s3])

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
