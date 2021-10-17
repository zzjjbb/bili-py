#!/usr/bin/env python3
import av
import sys
import os
import logging
import time
from fractions import Fraction
from queue import Queue
import threading
from abc import ABC, abstractmethod

logging.basicConfig(format='%(asctime)s [%(levelname).1s] %(message)s', level=logging.DEBUG)
logging.getLogger('libav').setLevel(logging.INFO)


def logging_refresh(*args, __refresh_interval=1, **kwargs):
    if getattr(logging_refresh, 'time', 0) + __refresh_interval < time.time():
        logging.log(*args, **kwargs)
        logging_refresh.time = time.time()


class AsyncStream(ABC):
    """
    Designed for encode in a separate thread for very slow encoders
    """
    stream = None
    container = None
    _queue = None

    def __init__(self, stream, maxsize=1000):
        self.stream = stream
        self.container = stream.container
        self._thread = threading.Thread(target=self.run)
        self._queue = Queue(maxsize)
        self._thread.start()

    def put(self, frame_info):
        self._queue.put(frame_info)
        if frame_info[0] is None:
            self._thread.join()

    @abstractmethod
    def _encode(self, frame_info):
        ...

    def run(self):
        while True:
            frame_info = self._queue.get()
            self._encode(frame_info)
            if frame_info[0] is None:
                break


class HQVideo(AsyncStream):
    def _encode(self, frame_info):
        frame, lock = frame_info
        with lock:
            new_packet = self.stream.encode(frame)
        self.container.mux(new_packet)


class CompactVideo(AsyncStream):
    _restart_every = float('inf')

    def __init__(self, *args, restart_every=0, **kwargs):
        if restart_every > 0:
            self._restart_every = restart_every
        self._frame_count = self._restart_every
        super().__init__(*args, **kwargs)

    def _encode(self, frame_info):
        self._frame_count -= 1
        frame, lock = frame_info
        with lock:
            new_packet = self.stream.encode(frame)
        self.container.mux(new_packet)
        if self._frame_count <= 0:
            self._frame_count = self._restart_every
            new_packet = self.stream.encode(None)
            self.container.mux(new_packet)
            self.stream.codec_context.close()
            self.stream.codec_context.open()


class Transcoder:
    def __init__(self, containers, infos):
        assert len(containers) == len(infos)
        self.containers = containers
        self.infos = infos
        self.decoders = {}
        self._input_info = {'time_base': {'video': None, 'audio': None}, 'pts_offset': {'video': 0, 'audio': 0}}
        self.dummy_packet = {}

    def init_with_template(self, template):
        t_v = template.streams.video[0]
        t_a = template.streams.audio[0]
        t_s = {'video': t_v, 'audio': t_a}

        # set stream for output containers
        for container, info in zip(self.containers, self.infos):
            if info.get('streams', False):  # complain if any content have been set in info['streams']
                raise NotImplementedError('not support reset stream after initialized')
            info['streams'] = {}
            if info['mode'] == 'origin':
                info['streams']['video'] = container.add_stream(template=t_v)
                info['streams']['audio'] = container.add_stream(template=t_a)
            elif info['mode'] == 'hq':
                v_o = {
                    'profile':     'high',
                    'tune':        'film',
                    'crf':         '25',
                    'mbtree':      '1',
                    'refs':        '10',
                    'g':           '240',
                    'keyint_min':  '1',
                    'bf':          '4',
                    'me_method':   'umh',
                    'subq':        '7',
                    'me_range':    '16',
                    'aq-mode':     '3',
                    'aq-strength': '0.8',
                    'psy-rd':      '0.7:0.1',
                    'qcomp':       '0.75',
                    'x264-params': 'rc-lookahead=120',
                    'threads':     '12',
                    'thread_type': 'frame'
                }
                out_v = container.add_stream('libx264', options=v_o, rate=t_v.framerate)
                out_v.pix_fmt = t_v.pix_fmt
                out_v.width = t_v.width
                out_v.height = t_v.height
                out_v.codec_context.time_base = Fraction(1, 1000000)
                info['streams']['video_async'] = HQVideo(out_v)
                info['streams']['audio'] = container.add_stream(template=t_a)
            elif info['mode'] == 'compact':
                a_o = {
                    'frame_duration':  '60',
                    'apply_phase_inv': '0',
                    'cutoff':          '20000',
                    'b':               '48000'
                }
                out_a = container.add_stream('libopus', options=a_o, rate=t_a.sample_rate)
                # out_a.time_base = out_a.codec_context.time_base = Fraction(1, t_a.sample_rate)
                info['streams']['audio'] = out_a
                v_o = {
                    'preset':   '8',
                    'qp':       '58',
                    'la_depth': '90',
                    'bsf':      'color_range=pc'
                }
                out_v = container.add_stream('libsvtav1', options=v_o, rate=t_v.guessed_rate)
                out_v.width = t_v.width
                out_v.height = t_v.height
                out_v.codec_context.time_base = Fraction(1, 1000000)
                info['streams']['video_async'] = CompactVideo(out_v, restart_every=1000)
                info['frame_count'] = {'audio': 0}

        for s in ['video', 'audio']:
            # use only one continuous decoder to avoid concatenating problems (e.g. eliminate AAC priming samples)
            decoder = t_s[s].codec_context.codec.create()
            decoder.extradata = t_s[s].codec_context.extradata
            self.decoders[s] = decoder
            # make dummy packets (used for flush decoder to yield proper time_base)
            packet = av.Packet()
            packet.time_base = t_s[s].time_base
            self.dummy_packet[s] = packet

    def append(self, in_vid):
        with av.open(in_vid, metadata_errors='ignore') as input_:
            if self.infos[0].get('streams', None) is None:  # should be either all None or all not None
                self.init_with_template(input_)
            streams_in = {}
            offset = {}
            for s in ['video', 'audio']:
                streams_in[s] = input_.streams.get({s: 0})[0]
                offset[s] = self._input_info['pts_offset'][s] - streams_in[s].start_time
                if self._input_info['time_base'][s] is None:
                    self._input_info['time_base'][s] = streams_in[s].time_base
                elif self._input_info['time_base'][s] != streams_in[s].time_base:
                    raise ValueError(f"video '{in_vid}' has different time base with previous ones!")

            pts_max = {'video': 0, 'audio': 0}
            total_time = float(input_.duration / av.time_base)
            for i, packet in enumerate(input_.demux()):
                if packet.dts is None:
                    continue  # dummy packages are useless for our custom decoders
                progress_time = float(max(0, packet.dts * packet.time_base))
                logging_refresh(logging.DEBUG, f"progress={progress_time / total_time * 100:.1f}% "
                                               f"time={progress_time:.1f}s/{total_time:.1f}s")
                s = packet.stream.type
                # reset packet info
                packet.pts += offset[s]
                packet.dts += offset[s]
                pts_max[s] = max(pts_max[s], packet.pts + packet.duration)
                # decode by custom decoders
                frames = self.decoders[s].decode(packet)
                # encode & mux
                for container, info in zip(self.containers, self.infos):
                    # CAUTION: recheck before modifying packet/frames information, they may be reused in other mode
                    if packet.stream.type == 'video':
                        if info['mode'] == 'origin':
                            if packet.dts is not None:
                                packet.stream = info['streams']['video']
                                container.mux(packet)
                        elif info['mode'] in ('hq', 'compact'):
                            for frame in frames:
                                frame.pict_type = 0
                                info['streams']['video_async'].put([frame, threading.Lock()])
                    if packet.stream.type == 'audio':
                        if info['mode'] in ['origin', 'hq']:
                            if packet.dts is not None:
                                packet.stream = info['streams']['audio']
                                container.mux(packet)
                        elif info['mode'] == 'compact':
                            for frame in frames:
                                frame.pts = None
                                new_packet = info['streams']['audio'].encode(frame)
                                for p in new_packet:
                                    p.time_base = Fraction(1, info['streams']['audio'].sample_rate)
                                    p.pts = p.dts = info['frame_count']['audio']
                                    info['frame_count']['audio'] += p.duration
                                container.mux(new_packet)
        self._input_info['pts_offset'] = pts_max  # save pts info for next input

    def flush_close(self):
        frames = {}
        for s in ['video', 'audio']:
            frames[s] = self.decoders[s].decode(self.dummy_packet[s])
            frames[s].append(None)  # to flush encoder
            self.decoders[s].close()
        for container, info in zip(self.containers, self.infos):
            if info['mode'] in ['hq', 'compact']:
                for frame in frames['video']:
                    if frame is not None:
                        frame.pict_type = 0
                    info['streams']['video_async'].put([frame, threading.Lock()])
            if info['mode'] == 'compact':
                for frame in frames['audio']:
                    if frame is not None:
                        frame.pts = None
                    new_packet = info['streams']['audio'].encode(frame)
                    for p in new_packet:
                        p.time_base = Fraction(1, info['streams']['audio'].sample_rate)
                        p.pts = p.dts = info['frame_count']['audio']
                        info['frame_count']['audio'] += p.duration
                    container.mux(new_packet)
            container.close()


in_dir = sys.argv[1]
out_dir = sys.argv[2]
vid_names = os.listdir(in_dir)
vid_names.sort(reverse=False)
out_list = [av.open(os.path.join(out_dir, name), mode='w') for name in ['origin.mp4', 'hq.mp4', 'compact.webm']]
out_info = [{'mode': m} for m in ['origin', 'hq', 'compact']]

transcoder = Transcoder(out_list, out_info)

for vid_name in vid_names:
    if vid_name[-3:] in ['flv', 'mp4']:
        logging.info(f"start processing '{vid_name}'")
        transcoder.append(os.path.join(in_dir, vid_name))

transcoder.flush_close()