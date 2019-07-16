#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Sep 03 22:32:05 2018

@author: david
"""
import regex as re

from ipaddress import ip_address


def process_bssid(v):
    try:
        match = re.match('([0-9a-fA-F]{2}(:|$)){6}', v)
        return v.upper() if match else None
    except:
        return None


def process_ip(v):
    try:
        return v if ip_address(v).is_global else None
    except:
        return None


def process_ssid(v):
    try:
        match = re.match('\\\"(.*)\\\"', v)
        return match.group(1) if match else v
    except:
        return None
