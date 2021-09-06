import av
from hashlib import md5
from io import BytesIO

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
    segment_md5_buffer = {'audio': BytesIO(), 'video': BytesIO()}
    segment_md5_containers = None
    keyframe_md5 = []
    with av.open(video_name, metadata_errors='ignore') as input_:
        check_av_stream(input_)
        for i, packet in enumerate(input_.demux()):
            # if i>3:
            #     break
            if packet.dts is None:
                continue
            # print(packet)

            if packet.stream.type == 'video' and packet.is_keyframe:
                print(i, float(packet.pts * packet.time_base))
                keyframe_md5.append(md5(packet).hexdigest())
                if segment_md5_containers is not None:
                    for c in segment_md5_containers.values():
                        c.close()
                    segment_md5_containers = None

            if segment_md5_containers is None:
                segment_md5_containers = {}
                for s in input_.streams:
                    segment_md5_containers[s.type] = av.open(segment_md5_buffer[s.type], 'w', format='md5')
                    segment_md5_containers[s.type].add_stream(template=s)
            packet.stream = segment_md5_containers[packet.stream.type].streams[0]
            segment_md5_containers[packet.stream.type].mux(packet)
        for s in segment_md5_containers.values():
            s.close()

    # post-processing
    # convert BytesIO to md5 values list and reformat to list of dicts
    segment_md5 = [segment_md5_buffer[s].getbuffer().tobytes().decode('ascii').strip().split('\n')
                   for s in ['audio', 'video']]
    segment_md5 = [{'audio': audio[4:], 'video': video[4:]} for audio, video in zip(*segment_md5)]
    # reformat all info to one output list
    out = []
    for seg_idx in range(len(keyframe_md5)):
        out.append({
            'keyframe_md5': keyframe_md5[seg_idx],
            'segment_md5':  segment_md5[seg_idx]
        })
    # out_buffer = out_buffer.getbuffer().tobytes().decode('ascii').strip()
    # return [i[4:] for i in out_buffer.split('\n')]
    return out


a = get_hash(
    r"E:\OneDrive - mail.ustc.edu.cn\A-SOUL_records\[210822] 【3D】小小梦魇（不通关就通宵） - 向晚大魔王\source\test.mp4")
print(a)
