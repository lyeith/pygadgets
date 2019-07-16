#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Sep 18 14:17:55 2018

@author: david
"""
import regex as re
import phonenumbers

from functools import lru_cache
from phonenumbers import geocoder, carrier, PhoneNumberType

PHONE_TYPE = {v: k for k, v in PhoneNumberType.__dict__.items()}


@lru_cache(maxsize=256)
def process_phone(phone_raw, country='ID', lang='en', metadata=True):

    if not phone_raw:
        return None

    if type(phone_raw) != str:
        phone_raw = str(phone_raw)

    if any(e.isalpha() or e in '#*' for e in phone_raw) or \
            len(phone_raw) <= 7:
        if not metadata:
            return phone_raw

        else:
            dct = {
                'phone': phone_raw,
                'phone_type': 'SHORT_CODE'
            }
            return dct

    phone = re.sub('[/;].*$', '', phone_raw)
    phone = re.sub('[^+#*0-9]', '', phone)

    if country == 'ID':
        phone = re.sub('^0062|^062', '+62', phone)
        phone = re.sub('^\+0', '0', phone)

        if phone in ('+62', '62'):
            return None

    try:
        x = phonenumbers.parse(phone, country)
        phone = '+{}{}'.format(x.country_code, x.national_number)

        if not metadata:
            return phone

        else:
            location = geocoder.description_for_number(x, lang)
            phone_type = PHONE_TYPE[phonenumbers.number_type(x)]
            telco = carrier.name_for_number(x, lang)

            dct = {
                'phone': phone,
                'phone_type': phone_type
            }

            if location:
                dct['location'] = location

            if telco:
                dct['carrier'] = telco

    except:
        if not metadata:
            return phone_raw

        else:
            dct = {
                'phone': phone_raw
            }

    return dct


def trans_phone(v, field=None):

    phone_dct = process_phone(v)
    if not phone_dct:
        return None

    if not field:
        dct = phone_dct

    else:
        dct = {
            '{}'.format(field): phone_dct.get('phone'),
            '{}_carrier'.format(field): phone_dct.get('carrier'),
            '{}_location'.format(field): phone_dct.get('location'),
            '{}_type'.format(field): phone_dct.get('phone_type'),
        }

    return dct


def process_is_incoming(v):
    try:
        v1 = int(v)
        return True if v1 == 1 else False if v1 == 0 else None
    except:
        return None


def main():
    print(process_phone('082232879690/085645443569', country='ID', metadata=True))


if __name__ == '__main__':
    main()
