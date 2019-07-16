#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Jan 18 14:15:12 2018

@author: david
"""
import difflib
import numpy as np
import regex as re

from collections import defaultdict
from ds_common.data import consts
from fuzzywuzzy import fuzz

NAME_NORMALIZE_LST = [
    ['mohamad', 'mohamed', 'mohammad', 'mohammed', 'muhamad',
     'muhammed', 'mohd', 'muh', 'muhammad', 'moh', 'md', 'mhd', 'mat', 'mo'],
    ['abdulla', 'abdullah', 'abdul', 'abd', 'abdu', 'abduh', 'abdi', 'abdal', 'abdel', 'abdil', 'ab']
]


def process_name(name_raw):

    def proc_prefix(lst, pref=list()):

        if not lst or lst[0] not in consts.NAME_PREFIXES:
            return lst, pref

        pref_cmb = ''.join(lst[0:2])
        if pref_cmb in consts.NAME_PREFIXES:
            return proc_prefix(lst[2:], pref + [pref_cmb])
        else:
            return proc_prefix(lst[1:], pref + [lst[0]])

    def proc_suffix(lst, suf=list()):

        if not lst:
            return lst, suf

        l = len(lst)
        neg_l = -l

        if lst[-1][0] in ('s', 'm') and lst[-1][1:] in consts.DEGREES:  # se
            return proc_suffix(lst[:-1], [lst[-1]] + suf)

        elif -2 >= neg_l and lst[-2] in ('s', 'm') and lst[-1] in consts.DEGREES:  # s.e
            return proc_suffix(lst[:-2], [''.join(lst[-2:])] + suf)
        elif -2 >= neg_l and lst[-2][0] in ('s', 'm') and ''.join([lst[-2][1:], lst[-1]]) in consts.DEGREES:  # spd.i
            return proc_suffix(lst[:-2], [''.join(lst[-2:])] + suf)
        elif -3 >= neg_l and lst[-3] in ('s', 'm') and ''.join(lst[-2:]) in consts.DEGREES:  # s.pd.i
            return proc_suffix(lst[:-3], [''.join(lst[-3:])] + suf)

        for i in reversed(range(l)):
            if lst[i] in ('amd', 'sst'):  # amd
                return proc_suffix(lst[:i], [' '.join(lst[i:])] + suf)
            elif i+1 < l and ''.join(lst[i:i+2]) in ('amd', 'sst'):  # a.md
                tmp = ' '.join(lst[i+2:])
                return proc_suffix(lst[:i], [''.join(lst[i:i+2]) + (' ' + tmp if tmp else '')] + suf)
            elif i+2 < l and ''.join(lst[i:i+3]) in ('amd', 'sst'):  # a.m.d
                tmp = ' '.join(lst[i+3:])
                return proc_suffix(lst[:i], [''.join(lst[i:i+3]) + (' ' + tmp if tmp else '')] + suf)

        return lst, suf

    if not name_raw or len(name_raw) <= 1:
        return None, None, None

    name = re.sub('\(.*\)', ' ', name_raw.lower())
    name = re.sub('[\'-]', '', name)
    name_lst = re.split('[^a-z]+', name)
    name_lst = [elem for elem in name_lst if elem]
    if not name_lst:
        return None, None, None

    name_lst, name_prefix = proc_prefix(name_lst)
    name_lst, name_suffix = proc_suffix(name_lst)

    name_prefix = name_prefix if name_prefix else None
    name = ' '.join(name_lst) if name_lst else None
    name_suffix = name_suffix if name_suffix else None

    return name_prefix, name, name_suffix


def name_match(name1, name2, ratio_high=0.8, ratio_low=0.7):

    if not name1 or not name2:
        res = 'MISMATCHED'
        return res

    for word_lst in NAME_NORMALIZE_LST:
        name1, name2 = name_normalize(name1, word_lst), name_normalize(name2, word_lst)

    sim = difflib.SequenceMatcher(lambda x: x == ' ', ''.join(name1.split()), ''.join(name2.split())).ratio()
    if sim >= ratio_high:
        res = 'MATCHED'
        return res

    l1, l2 = name1.split(), name2.split()
    if len(l1) > len(l2):
        l1, l2 = l2, l1
    n, m = len(l1), len(l2)

    sim_mat = np.empty((n, m))
    for i in range(n):
        w1 = l1[i]
        for j in range(m):
            w2 = l2[j]

            if len(w1) == 1:
                sim_mat[i, j] = 1. if w1 == w2[0] else 0.
            elif len(w2) == 1:
                sim_mat[i, j] = 1. if w2 == w1[0] else 0.
            else:
                sim_mat[i, j] = difflib.SequenceMatcher(lambda x: x == ' ', w1, w2).ratio()

    sim = np.nanmax(sim_mat, axis=1).sum() / n

    if sim >= ratio_high:
        res = 'MATCHED'
        return res

    name1, name2 = name_expand_abr(name1, name2)
    sim = fuzz.token_set_ratio(name1, name2) / 100

    res = 'MATCHED' if sim >= ratio_high else 'MISMATCHED' if sim < ratio_low else 'SIMILAR'

    return res


# Note that token ordering is not preserved
def name_expand_abr(name1, name2, abr_max_len=2):

    if name1 in (None, 'none') or name2 in (None, 'none'):
        return name1, name2

    token1, token2 = set(name1.split()), set(name2.split())

    diff = token1 ^ token2
    prob_abr = [e for e in diff if 1 < len(e) <= abr_max_len]
    diff = diff ^ set(prob_abr)

    word_matches, abr_matches = [], []

    for abr in prob_abr:
        word_match = defaultdict(list)
        for letter in abr:
            for word in diff:
                if len(word) > 1 and word[0] == letter:
                    word_match[letter].append(word)

        if len(word_match) == len(abr) and \
                all(len(v) == 1 for k, v in word_match.items()):
            word_matches.append([v[0] for k, v in word_match.items()])
            abr_matches.append(abr)

    word_matches_filtered = []
    for abr in abr_matches:
        # Check that abbreviation is in the opposite name from matched words
        for matches in word_matches:
            if (abr in name1 and all(word in name2 for word in matches)) or \
                    (abr in name2 and all(word in name1 for word in matches)):
                word_matches_filtered.append(matches)

    if len(word_matches_filtered) == 1:
        word_match, abr_match = word_matches_filtered[0], abr_matches[0]

        token1.discard(abr_match)
        token2.discard(abr_match)
        token1 = token1 | set(word_match)
        token2 = token2 | set(word_match)

        left = ' '.join(token1)
        right = ' '.join(token2)
        return left, right

    else:
        return name1, name2


def name_normalize(name, word_lst):
    if name is None:
        return name
    if word_lst is None:
        return name

    lst = name.split()

    for k, v in enumerate(lst):
        if v.lower() in word_lst:
            lst[k] = word_lst[0]
        else:
            lst[k] = lst[k].lower()

    name = ' '.join(lst)
    return name
