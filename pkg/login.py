import requests

class Account:
    def __init__(self, sess):
        self.sess = sess

    def get_info(self):
        url = "http://api.bilibili.com/x/web-interface/nav"
        cookies = {
            'SESSDATA': self.sess
        }
        response = requests.get(url, cookies=cookies)
        return response

    def get_video_url(self):
        pass
