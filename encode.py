#!/usr/bin/env python3
import av
import sys
import os
import logging
import time
from fractions import Fraction
from queue import Queue, PriorityQueue, Empty
import warnings
import random
import threading
from abc import ABC, abstractmethod
import argparse

logging.basicConfig(format='%(asctime)s [%(levelname).1s] [%(name)s] %(message)s', level=logging.DEBUG)
logging.getLogger('libav').setLevel(logging.INFO)


def logging_refresh(refresh_interval=1):
    def _func(*args, **kwargs):
        if _func.time + refresh_interval < time.time():
            logging.log(*args, **kwargs)
            _func.time = time.time()

    _func.time = 0
    return _func


def copy_packet(packet):
    new_packet = av.Packet(packet)
    new_packet.time_base = packet.time_base
    new_packet.pts = packet.pts
    new_packet.dts = packet.dts
    return new_packet


class AsyncStream(ABC):
    """
    Designed for encode in a separate thread for very slow encoders
    """
    stream = None
    container = None
    _queue = None
    _alive = True

    def __init__(self, stream, maxsize=300):
        self.stream = stream
        self.container = stream.container
        self._thread = threading.Thread(target=self.run)
        self._queue = Queue(maxsize)
        self._mux_queue = PriorityQueue()
        self._unmuxed_video = 0
        self._finish_flag = False
        self._thread.start()

    def put(self, frame):
        self._queue.put(frame)

    @abstractmethod
    def _encode(self, frame):
        ...

    def put_mux_queue(self, packets):
        if isinstance(packets, av.Packet):
            packets = [packets]
        for p in packets:
            assert isinstance(p, av.Packet)
            self._mux_queue.put((p.dts * p.time_base + random.random() * 0.001, p))
            if p.stream.type == 'video':
                self._unmuxed_video += 1

    def _mux_flush(self, flush_all=False):
        try:  # mux when there are unmuxed video frames / forced flush all
            while self._unmuxed_video > 10 or flush_all:
                p = self._mux_queue.get_nowait()[1]
                self.container.mux_one(p)
                if p.stream.type == 'video':
                    self._unmuxed_video -= 1
        except Empty:
            if not flush_all:
                warnings.warn("reach end of mux_queue without flush_all")

    def run(self):
        while self._alive:  # main encoding loop
            frame = self._queue.get()
            self._encode(frame)
            self._mux_flush()  # just call it periodically
            if frame is None:
                break
        else:
            return  # force stop, exit instantly

        # getting an empty packet means reaching the end
        while not self._finish_flag:  # wait for finish signal
            time.sleep(0.1)
        self._mux_flush(True)  # until mux everything

    def wait_until_finish(self):
        self._finish_flag = True
        self._thread.join()

    def force_stop(self):
        self._alive = False
        self._thread.join()


class HQVideo(AsyncStream):
    def __init__(self, *args, **kwargs):
        self.logger = logging_refresh(10)
        super().__init__(*args, **kwargs)

    def _encode(self, frame):
        self.put_mux_queue(self.stream.encode(frame))
        self.logger(logging.DEBUG, f"{self}: queue size {self._queue.qsize()}")


class CompactVideo(AsyncStream):
    _restart_every = float('inf')

    def __init__(self, *args, restart_every=0, options, **kwargs):
        if restart_every > 0:
            self._restart_every = restart_every
        self._frame_count = self._restart_every
        self.logger = logging_refresh(10)
        self.options = options
        super().__init__(*args, **kwargs)

    def _encode(self, frame):
        self._frame_count -= 1
        self.put_mux_queue(self.stream.encode(frame))
        if self._frame_count <= 0:
            self._frame_count = self._restart_every
            self.put_mux_queue(self.stream.encode(None))
            self.stream.codec_context.close()
            if self.options is not None:
                self.stream.codec_context.options = self.options
            self.stream.codec_context.open()
        self.logger(logging.DEBUG, f"{self}: queue size {self._queue.qsize()}")


class Transcoder:
    def __init__(self, containers, infos, ignore_video_pts=False):
        assert len(containers) == len(infos)
        self.containers = containers
        self.infos = infos
        self.decoders = {}
        self._input_info = {'time_base': {'video': None, 'audio': None}, 'pts_offset': {'video': 0, 'audio': 0}}
        self.dummy_packet = {}
        self.ignore_video_pts = ignore_video_pts
        self.video_frame_pts = None

    def init_with_template(self, template):
        """
        Add audio/video streams to containers according to template (also according to mode name)
        :param template: input stream of the transcoder
        """
        t_v = template.streams.video[0]
        t_a = template.streams.audio[0]
        t_s = {'video': t_v, 'audio': t_a}

        def copy_format_info(src, dst):
            dst.width = src.width
            dst.height = src.height
            dst.sample_aspect_ratio = src.sample_aspect_ratio
            dst.pix_fmt = "yuv420p"
            dst.codec_context.color_range = 1
            dst.codec_context.color_primaries = src.codec_context.color_primaries
            dst.codec_context.color_trc = src.codec_context.color_trc
            dst.codec_context.colorspace = src.codec_context.colorspace
            # if pix_fmt:
            #     dst.pix_fmt = src.pix_fmt
            #     dst.codec_context.color_primaries = src.codec_context.color_primaries
            #     dst.codec_context.color_trc = src.codec_context.color_trc
            #     dst.codec_context.colorspace = src.codec_context.colorspace
            #     dst.codec_context.color_range = src.codec_context.color_range

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
                    'g':           '480',
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
                    'threads':     '3',
                    'thread_type': 'frame'
                }
                out_v = container.add_stream('libx264', options=v_o, rate=t_v.framerate)
                copy_format_info(t_v, out_v)
                out_v.codec_context.time_base = Fraction(1, 48000)
                info['streams']['async'] = HQVideo(out_v)
                info['streams']['audio'] = container.add_stream(template=t_a)
            elif info['mode'] == 'compact':
                a_o = {
                    'frame_duration':  '60',
                    'apply_phase_inv': '0',
                    'cutoff':          '20000',
                    'b':               '48000'
                }
                out_a = container.add_stream('libopus', options=a_o, rate=48000)
                # out_a.time_base = out_a.codec_context.time_base = Fraction(1, t_a.sample_rate)
                info['streams']['audio'] = out_a
                v_o = {
                    'preset':        '5',
                    'crf':           '50',
                    'svtav1-params': 'tune=0:lp=6:pin=0'
                }
                out_v = container.add_stream('libsvtav1', options=v_o, rate=t_v.guessed_rate)
                copy_format_info(t_v, out_v)
                out_v.codec_context.time_base = Fraction(1, 48000)
                info['streams']['async'] = CompactVideo(out_v, options=v_o)
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

    def process_frames(self, frames, frame_type):
        processed = []
        for frame in frames:
            if frame_type == 'video':
                if self.video_frame_pts is None:  # first frame
                    self.video_frame_pts = frame.pts
                else:
                    if self.ignore_video_pts:  # generate 60fps video starting from the first pts value
                        self.video_frame_pts += int(1 / 60 / frame.time_base)
                        frame.pts = self.video_frame_pts
                    else:  # if not ignore, must ensure frame pts is monotonic before sent to the encoder
                        if frame.pts <= self.video_frame_pts:
                            logging.warning("Decoder gives non monotonically increasing frame pts. Skipped")
                            continue
                        self.video_frame_pts = frame.pts
                # I/P/B frame should be decided by the encoder rather than the source
                frame.pict_type = 0
            elif frame_type == 'audio':
                if frame is not None:
                    # the audio encoder will take care of the audio packet pts
                    frame.pts = None
            processed.append(frame)
        return processed

    def append(self, in_vid):
        with av.open(in_vid, metadata_errors='ignore') as input_:
            if self.infos[0].get('streams', None) is None:  # should be either all None or all not None
                self.init_with_template(input_)
            streams_in = {}
            offset = {}
            start_time = {}
            for s in ['video', 'audio']:
                streams_in[s] = input_.streams.get({s: 0})[0]
                offset[s] = self._input_info['pts_offset'][s]
                if self._input_info['time_base'][s] is None:
                    self._input_info['time_base'][s] = streams_in[s].time_base
                elif self._input_info['time_base'][s] != streams_in[s].time_base:
                    raise ValueError(f"file '{in_vid}' has different time base with previous ones!")
            pts_max = {'video': 0, 'audio': 0}
            total_time = float(input_.duration / av.time_base)
            progress_logger = logging_refresh()
            for i, packet in enumerate(input_.demux()):
                if packet.dts is None:
                    continue  # dummy packages are useless for our custom decoders
                s = packet.stream.type
                # reset packet info
                if start_time.get(s) is None:
                    start_time[s] = packet.pts
                    offset[s] -= start_time[s]
                progress_time = float(max(0, (packet.dts - start_time[s]) * packet.time_base))
                progress_logger(logging.DEBUG, f"progress={progress_time / total_time * 100:.1f}% "
                                               f"time={progress_time:.1f}s/{total_time:.1f}s")
                packet.pts += offset[s]
                packet.dts += offset[s]
                pts_max[s] = max(pts_max[s], packet.pts + packet.duration)
                # decode by custom decoders
                frames = self.process_frames(self.decoders[s].decode(packet), s)
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
                                info['streams']['async'].put(frame)
                    if packet.stream.type == 'audio':
                        if info['mode'] == 'origin':
                            if packet.dts is not None:
                                packet.stream = info['streams']['audio']
                                container.mux(packet)
                        elif info['mode'] == 'hq':
                            if packet.dts is not None:
                                new_packet = copy_packet(packet)
                                new_packet.stream = info['streams']['audio']
                                info['streams']['async'].put_mux_queue(new_packet)
                        elif info['mode'] == 'compact':
                            for frame in frames:
                                new_packet = info['streams']['audio'].encode(frame)
                                for p in new_packet:
                                    p.time_base = Fraction(1, info['streams']['audio'].sample_rate)
                                    p.pts = p.dts = info['frame_count']['audio']
                                    info['frame_count']['audio'] += p.duration
                                    info['streams']['async'].put_mux_queue(new_packet)
        self._input_info['pts_offset'] = pts_max  # save pts info for next input

    def flush_close(self):
        frames = {}
        for s in ['video', 'audio']:
            frames[s] = self.decoders[s].decode(self.dummy_packet[s])
            frames[s] = self.process_frames(frames[s], s)
            frames[s].append(None)  # to flush encoder
            self.decoders[s].close()
        for container, info in zip(self.containers, self.infos):
            if info['mode'] == 'compact':
                for frame in frames['audio']:
                    new_packet = info['streams']['audio'].encode(frame)
                    for p in new_packet:
                        p.time_base = Fraction(1, info['streams']['audio'].sample_rate)
                        p.pts = p.dts = info['frame_count']['audio']
                        info['frame_count']['audio'] += p.duration
                    info['streams']['async'].put_mux_queue(new_packet)
            if info['mode'] in ['hq', 'compact']:
                for frame in frames['video']:
                    info['streams']['async'].put(frame)
                info['streams']['async'].wait_until_finish()
            container.close()

    def force_close(self):
        for info in self.infos:
            if (s := info['streams'].get('async')) is not None:
                s.force_stop()
        for c in self.containers:
            for s in c.streams:
                if s.codec_context.is_encoder:
                    s.encode(None)
            c.close()
        for s in ['video', 'audio']:
            self.decoders[s].decode(self.dummy_packet[s])
            self.decoders[s].close()


parser = argparse.ArgumentParser(description="encode video as H.264/AV1 and mux in mp4/webm [v230401]")
parser.add_argument('src', help="source directory for input videos")
parser.add_argument('dst', help="destination directory for output videos")
parser.add_argument('-t', '--type', metavar='O/H/C',
                    help='use letter(s) to control which file will be generated (default is all)')
parser.add_argument('--ignore_video_pts', action='store_true',
                    help="remove the input pts info and generate 60fps video (only affect re-encoded files H/C)")

cli_args = parser.parse_args()
in_dir = cli_args.src
out_dir = cli_args.dst
if cli_args.type is None:
    out_type = 'OHC'
else:
    out_type = [t for t in 'OHC' if t in cli_args.type.upper()]
ignore_video_pts = cli_args.ignore_video_pts

vid_names = os.listdir(in_dir)
vid_names.sort(reverse=False)
out_list = [av.open(os.path.join(out_dir, {'O': 'origin.mp4', 'H': 'hq.mp4', 'C': 'compact.webm'}[t]), mode='w')
            for t in out_type]
out_info = [{'mode': {'O': 'origin', 'H': 'hq', 'C': 'compact'}[t]} for t in out_type]

if not vid_names:
    logging.warning(f"no output video. exiting")
    sys.exit(1)

transcoder = Transcoder(out_list, out_info, ignore_video_pts)

if not vid_names:
    logging.error(f"no valid source video. exiting")
    sys.exit(1)

try:
    for vid_name in vid_names:
        if vid_name[-3:] in ['flv', 'mp4']:
            logging.info(f"start processing '{vid_name}'")
            transcoder.append(os.path.join(in_dir, vid_name))
except BaseException as e:
    logging.error(f"Encounter an error. Trying to flush and close")
    transcoder.force_close()
    raise e

transcoder.flush_close()
