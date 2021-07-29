#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Jan 18 14:14:33 2018

@author: david
"""
import difflib
import regex as re


def process_company(company_raw):

    if not company_raw:
        return None

    company = re.sub('[(\[].*[)\]]', ' ', company_raw.lower())
    company = re.sub('^pt\W+|\W+(pt\W*|tbk\W*)*\W*$', '', company)

    # Exceptions
    if company in ('bank indonesia',):
        pass

    else:
        company = re.sub('\W+indonesia\W*$', '', company)

    company = ' '.join(re.split('\W+', company)).strip()

    return company


def company_match(company1, company2, ratio=0.8):

    if not company1 or not company2:
        return False

    sim = difflib.SequenceMatcher(lambda x: x == ' ', ''.join(company1.split()), ''.join(company2.split())).ratio()
    if sim > ratio:
        return True

    sim = difflib.SequenceMatcher(lambda x: x in ' .,', company1, company2).ratio()
    if sim > ratio:
        return True
