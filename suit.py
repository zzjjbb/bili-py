import requests
import re
import logging
import time
import json

logging.basicConfig(format='%(asctime)s [%(levelname).1s] %(message)s', level=logging.INFO)

base_url = "https://api.vc.bilibili.com/topic_svr/v1/topic_svr/topic_history"
table = {}
offset = 576501494364197725
try:
    for _ in range(5000):
        response = requests.get(base_url, {"topic_name":        "向晚大魔王",
                                           "offset_dynamic_id": offset})
        raw_data = response.json()
        if raw_data['code'] == 0:
            offset = raw_data['data']['offset']
            raw_data = raw_data['data']['cards']
            all_count = len(raw_data)
            valid_count = 0
            for card in raw_data:
                logging.debug("send_time " +
                              time.strftime('%m/%d %H:%M:%S GMT+8', time.gmtime(card['desc']['timestamp'] + 8 * 3600)))
                m = re.search('"description":"'r"我是#向晚大魔王#的NO.(\d{6})号真爱粉，靓号在手", card['card'])
                if m is not None:
                    valid_count += 1
                    num = m.group(1)
                    info = {'uid':        card['desc']['uid'],
                            'dynamic_id': card['desc']['dynamic_id'],
                            'timestamp':  card['desc']['timestamp']}
                    if table.get(num, None) is not None:
                        if table[num]['uid'] != card['desc']['uid']:
                            logging.warning(f'suit number {num}: duplicate users are found')
                            if other_list := table[num].get('other', None) is None:
                                table[num]['other'] = [info]
                            else:
                                other_list.append(info)
                        else:
                            continue
                    table[num] = info
            if all_count is None:
                logging.error(f'get error code 0 but get no data!')
            else:
                logging.info(f"get {valid_count}/{all_count}, total {len(table)}, earliest " +
                             time.strftime('%m/%d %H:%M:%S GMT+8', time.gmtime(card['desc']['timestamp'] + 8 * 3600)))
        else:
            logging.error(f"error code {raw_data['code']}, message {raw_data['message']}")
except BaseException as e:
    logging.error(f"Get unknown error. Exiting and write existing data to '{offset}.json'")
    raise e
finally:
    with open(f"{offset}.json", 'w') as f:
        json.dump(table, f)
