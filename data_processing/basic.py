#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Jun 13 11:18:05 2018

@author: david
"""
import regex as re


def process_bool(v):
    return v if type(v) == bool else None


def process_email(email_raw):

    if not email_raw:
        return None

    email = re.findall('[a-z0-9_.+-]+@[a-z0-9-]+\.[a-z0-9-.]+', email_raw.lower())
    email = email[0] if email else None

    return email


def process_decimal(v):
    try:
        return float(round(float(v), 2))
    except:
        return None


def process_float(v):
    try:
        return float(v)
    except:
        return None


def process_gps(v):
    try:
        if type(v) == tuple and len(v) == 2 and \
                90. >= float(v[0]) >= -90. and 180. >= float(v[1]) >= -180.:
            return float(v[0]), float(v[1])
        else:
            return None
    except:
        return None


def process_int(v):
    try:
        return int(float(v))
    except:
        return None


def process_string(v):
    return v.strip() if type(v) == str and v.strip() else None
