#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Jul 10 15:50:33 2019

@author: david
"""
import requests
import simplejson as json


def request_gps(addr, ggmap_token):  # [lat, lon]

    addr = addr.replace(' ', '+')
    url = 'https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'.format(addr, ggmap_token)
    response = requests.get(url, timeout=10)

    if response.ok:
        data = response.json()
        gps = list(data['results'][0]['geometry']['location'].values()) if data['status'] == 'OK' else None

        return gps

    else:
        raise Exception('GOOGLE MAP API FAILED!')


def google_translate(str_lst, ggtrans_key, source='id', target='en'):

    data = {
        'q': str_lst,
        'source': source,
        'target': target,
        'format': 'text'
    }

    url = 'https://translation.googleapis.com/language/translate/v2?key={}'.format(ggtrans_key)
    response = requests.post(url, data=json.dumps(data), timeout=100)

    if response.ok:

        info = json.loads(response.text)
        trans_lst = [elem['translatedText'] for elem in info['data']['translations']]

        return trans_lst

    else:
        raise Exception('GOOGLE TRANSLATE API FAILED!')
