#!/usr/bin/env python3
import json
import os.path
import sys
from hashlib import md5
from io import BytesIO
import av
import time
import argparse


def check_av_stream(container):
    """
    Assert container must have exactly 1 video stream and 1 audio stream.

    :param container: av.container.Container object for checking
    :return: None
    """
    stream_types = [s.type for s in container.streams]
    stream_types.sort()
    if stream_types != ['audio', 'video']:
        raise ValueError(f'cannot process streams: {stream_types}')


def print_refresh(*args, **kwargs):
    if getattr(print_refresh, 'time', 0) + 0.5 < time.time():
        print(*args, **kwargs, end='\r')
        print_refresh.time = time.time()


def get_hash(video_name):
    def close_last_segment():
        nonlocal segment_md5_containers, max_pts
        if segment_md5_containers is not None:
            keyframe_info[-1]['max_pts'] = max_pts
            keyframe_info[-1]['frames'] = {}
            for s in ['audio', 'video']:
                keyframe_info[-1]['frames'][s] = segment_md5_containers[s].streams[0].frames
                segment_md5_containers[s].close()
            segment_md5_containers = None

    segment_md5_buffer = {'audio': BytesIO(), 'video': BytesIO()}
    segment_md5_containers = None
    keyframe_info = []
    max_pts = -1000000
    # collect = {'dts': [], 'pts': [], 'dur': []}

    with av.open(video_name, metadata_errors='ignore') as input_:
        check_av_stream(input_)
        time_base = {s: float(getattr(input_.streams, s)[0].time_base) for s in ['audio', 'video']}
        for i, packet in enumerate(input_.demux()):
            packet: av.packet.Packet
            # if i>3:
            #     break
            if packet.dts is None:
                continue
            # print(packet)

            if packet.stream.type == 'video':
                if packet.is_keyframe:
                    print_refresh(f"packet {i}, time {float(packet.pts * packet.time_base):.3f}s")
                    close_last_segment()
                    keyframe_info.append({'pts': packet.pts, 'md5': md5(packet).hexdigest()})
                max_pts = max(max_pts, packet.pts)
                # collect['pts'].append(packet.pts)
                # collect['dts'].append(packet.dts)
                # collect['dur'].append(packet.duration)

            if segment_md5_containers is None:
                segment_md5_containers = {}
                for s in input_.streams:
                    segment_md5_containers[s.type] = av.open(segment_md5_buffer[s.type], 'w', format='md5')
                    segment_md5_containers[s.type].add_stream(template=s)
            packet.stream = segment_md5_containers[packet.stream.type].streams[0]
            segment_md5_containers[packet.stream.type].mux(packet)
        close_last_segment()
    print(' ' * 79 + '\r' + f"{os.path.basename(video_name)} finished")

    # post-processing
    # convert BytesIO to md5 values list and reformat to list of dicts
    segment_md5 = [segment_md5_buffer[s].getbuffer().tobytes().decode('ascii').strip().split('\n')
                   for s in ['audio', 'video']]
    segment_md5 = [{'audio': audio[4:], 'video': video[4:]} for audio, video in zip(*segment_md5)]
    # reformat all info to one output list
    out = []
    for seg_idx in range(len(keyframe_info)):
        out.append({
            'start_pts':    keyframe_info[seg_idx]['pts'],
            'end_pts':      keyframe_info[seg_idx]['max_pts'],
            'frames':       keyframe_info[seg_idx]['frames'],
            'keyframe_md5': keyframe_info[seg_idx]['md5'],
            'segment_md5':  segment_md5[seg_idx]
        })
    # out_buffer = out_buffer.getbuffer().tobytes().decode('ascii').strip()
    # return [i[4:] for i in out_buffer.split('\n')]
    return {'name': os.path.basename(video_name), 'time_base': time_base, 'data': out}


parser = argparse.ArgumentParser(description="hash video or video sequences segmented by key frame")
parser.add_argument('src', help="source file/directory for input video(s)")
parser.add_argument('out', help="output path for json file")
cli_args = parser.parse_args()

src_path = cli_args.src
out_path = cli_args.out

all_info = []
# pre-check file write access
open(out_path, 'a').close()

# dir as a sequence of videos
if os.path.isdir(src_path):
    vid_names = os.listdir(src_path)
    vid_names.sort()
    for vid_name in vid_names:
        if vid_name[-3:] in ['flv', 'mp4']:
            all_info.append(get_hash(os.path.join(src_path, vid_name)))
else:  # single video also OK
    all_info.append(get_hash(src_path))

with open(out_path, 'w') as f:
    json.dump(all_info, f, indent=1)
