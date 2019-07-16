#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Feb 01 14:11:28 2019

@author: david
"""
import setuptools

setuptools.setup(
    name='pygadgets',
    version='0.1.163',
    author='David Wong',
    author_email='david.wong.jm@outlook.com',
    description='Useful Python Gadgets',
    long_description='Mostly for use in indonesia',
    long_description_content_type='text/markdown',
    url='https://github.com/lyeith/pygadgets',
    packages=setuptools.find_packages(),
    install_requires=[
        'cassandra-driver',
        'confluent-kafka',
        'flask',
        'fuzzywuzzy',
        'gunicorn',
        'json_log_formatter',
        'newrelic',
        'numpy',
        'phonenumbers',
        'psycopg2-binary',
        'pykafka',
        'pymongo',
        'pymysql',
        'python-dateutil',
        'python-Levenshtein',
        'regex',
        'requests',
        'simplejson',
        'sklearn',
        'pyyaml',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent'
    ],
)
