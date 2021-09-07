import json
import os.path
import sys
from hashlib import md5
from io import BytesIO
import av


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


def get_hash(video_name):
    def close_last_segment():
        nonlocal segment_md5_containers
        if segment_md5_containers is not None:
            keyframe_info[-1]['frames'] = {}
            for s in ['audio', 'video']:
                keyframe_info[-1]['frames'][s] = segment_md5_containers[s].streams[0].frames
                segment_md5_containers[s].close()
            segment_md5_containers = None

    segment_md5_buffer = {'audio': BytesIO(), 'video': BytesIO()}
    segment_md5_containers = None
    keyframe_info = []

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

            if packet.stream.type == 'video' and packet.is_keyframe:
                print(f"packet {i}, time {float(packet.pts * packet.time_base):.3f}s", end='\r')
                close_last_segment()
                keyframe_info.append({'pts': packet.pts, 'md5': md5(packet).hexdigest()})

            if segment_md5_containers is None:
                segment_md5_containers = {}
                for s in input_.streams:
                    segment_md5_containers[s.type] = av.open(segment_md5_buffer[s.type], 'w', format='md5')
                    segment_md5_containers[s.type].add_stream(template=s)
            packet.stream = segment_md5_containers[packet.stream.type].streams[0]
            segment_md5_containers[packet.stream.type].mux(packet)
        close_last_segment()
    print(f"{os.path.basename(video_name)} finished")

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
            'frames':       keyframe_info[seg_idx]['frames'],
            'keyframe_md5': keyframe_info[seg_idx]['md5'],
            'segment_md5':  segment_md5[seg_idx]
        })
    # out_buffer = out_buffer.getbuffer().tobytes().decode('ascii').strip()
    # return [i[4:] for i in out_buffer.split('\n')]
    return {'name': os.path.basename(video_name), 'time_base': time_base, 'data': out}


base_dir = sys.argv[1]
out_path = sys.argv[2]

all_info = []
for vid_name in os.listdir(base_dir):
    if vid_name[-3:] in ['flv', 'mp4']:
        all_info.append(get_hash(os.path.join(base_dir, vid_name)))
with open(out_path, 'w') as f:
    json.dump(all_info, f, indent=1)
