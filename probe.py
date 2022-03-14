#!/usr/bin/env python3
import logging
import os
import ffmpeg
import glob
import json
import argparse

logging.basicConfig(format='%(asctime)s [%(levelname).1s] %(message)s', level=logging.INFO)

parser = argparse.ArgumentParser(description="attach media information in 'play' file")
parser.add_argument('path', help="source directory/directories (glob pattern)")
parser.add_argument('-f', '--force', action='store_true',
                    help="overwrite 'media_info' even if it already exists")
cli_args = parser.parse_args()

for rec_dir in glob.glob(cli_args.path):
    logging.info("processing %s", rec_dir)
    play_name = os.path.join(rec_dir, "play")
    try:
        with open(play_name, encoding='utf8') as play_f:
            play = json.load(play_f)
    except OSError as e:
        logging.error("'%s' is an invalid play file, skipped", play_name)
    if play.get('media_info') and not cli_args.force:
        logging.warning("not overwriting '%s', skipped", play_name)
        continue
    play['media_info'] = []
    for quality in play['video']['quality']:
        info = {'format': {}, 'video': {}, 'audio': {}}
        try:
            logging.debug("quality: %s, url %s", quality['name'], quality['url'])
            p = ffmpeg.probe(os.path.join(rec_dir, quality['url']))
        except ffmpeg.Error as e:
            if b"Invalid data found when processing input" in e.stderr:
                logging.error("invalid video file: %s", os.path.join(rec_dir, quality['url']))
                continue
            else:
                print(e.stderr.decode())
                raise e
        if sorted([s['codec_type'] for s in p['streams']]) != ['audio', 'video']:
            raise ValueError('not A/V streams')
        for stream in p['streams']:
            s_type = stream['codec_type']
            for tag in [
                # both
                'index', 'codec_name', 'codec_long_name', 'tags', 'profile', 'time_base', 'start_pts', 'start_time',
                'duration_ts', 'duration', 'bit_rate', 'nb_frames', 'r_frame_rate',
                # audio
                'sample_fmt', 'sample_rate', 'channels', 'channel_layout',
                # video
                'width', 'height', 'coded_width', 'coded_height', 'sample_aspect_ratio', 'display_aspect_ratio',
                'pix_fmt', 'level', 'color_range'
            ]:
                try:
                    info[s_type][tag] = stream[tag]
                except KeyError:
                    pass
        for tag in ['format_name', 'format_long_name', 'start_time', 'duration', 'size', 'bit_rate', 'tags']:
            info['format'][tag] = p['format'][tag]

        play['media_info'].append({'name': quality['name'], 'info': info})
    with open(os.path.join(rec_dir, "play"), 'w', encoding='utf8') as play_f:
        json.dump(play, play_f, indent=2, ensure_ascii=False)
