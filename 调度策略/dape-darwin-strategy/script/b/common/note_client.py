#!/usr/bin/env python

import requests
import json
import ssl
import logging
from datetime import datetime
ssl._create_default_https_context = ssl._create_unverified_context

logger = logging.getLogger(__name__)


class NoteClient(object):
    DCHAT_NTOE_TYPE = 'dchat'
    STDOUT_NOTE_TYPE = 'stdout'

    def __init__(self, conf):
        self._note_type = conf['common']['note_type']
        if self._note_type == self.DCHAT_NTOE_TYPE:
            self._dchat_not_urls = {
                'error': conf['common']['dchat_note_url_error'],
                'fatal': conf['common']['dchat_note_url_fatal'],
                'info': conf['common']['dchat_note_url_info'],
                'budget': conf['common']['dchat_note_url_budget'],
                'fun': conf['common']['dchat_note_url_fun'],
                'ROI': conf['common']['dchat_note_url_ROI'],
                'ROI_Message': conf['common']['dchat_note_url_ROI_Fine'],
                'MPS': conf['common']['dchat_note_url_you']
            }
        else:
            self._dchat_not_urls = {}

    def send_message(self, title, content, level='info'):
        if self._note_type == self.DCHAT_NTOE_TYPE:
            d_url = self._dchat_not_urls[level]
            dtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            headers = {"Content-Type": "application/json;charset=utf-8 "}
            msg = {
                "text": title,
                "attachments": [
                    {"title": "发布时间：%s" % dtime, "text": content, "color": "#123455"}
                ]
            }
            msg_txt = json.dumps(msg)
            requests.post(d_url, data=msg_txt, headers=headers)
        elif self._note_type == "stdout":
            print(title)
            print(content)
