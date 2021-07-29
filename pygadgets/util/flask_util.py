#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Jul 10 15:49:31 2019

@author: david
"""
import gunicorn.app.base
import logging.handlers
import os
import sys
import traceback

from pygadgets.util import gadgets, email_util, db_util
from flask import jsonify
from functools import partial


def init_app(app):

    logger = logging.getLogger('gunicorn')
    app.logger.handlers = logger.handlers

    app.logger.setLevel(logging.DEBUG if os.environ['ENV'] == 'DEV' else logging.INFO)
    app.json_encoder = gadgets.CustomerFlaskJSONEncoder
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

    app.add_url_rule('/', 'prefix', prefix)
    app.add_url_rule('/favicon.ico', 'favicon', favicon)
    app.add_url_rule('/healthcheck', 'healthcheck', healthcheck)

    app.register_error_handler(Exception, partial(handle_exception, app=app))
    app.register_error_handler(MemoryError, handle_memory_error)

    return app


def handle_memory_error(_):
    sys.exit(1)


def handle_exception(_, app):

    tb = traceback.format_exc()
    app.logger.error(tb)

    if os.environ['ENV'] in ('DEV', 'TEST', 'PREPROD'):
        res = {
            'msg': (tb + '\n').split('\n'),
            'status': 500,
        }
        return jsonify(res)

    mail_stat = post_crash_email(tb, recipients=None, mail_server={}, app_name=None)

    app.logger.error(mail_stat + '\n')
    res = {
        'msg': (tb + '\n' + mail_stat).split('\n'),
        'status': 500,
    }
    return jsonify(res)


def post_crash_email(tb, recipients, mail_server, app_name=None):

    env = os.environ['ENV']
    subject = 'Crash Reporting - {}'.format(app_name)
    body = 'Environment: {}\n\n'.format(env) + tb

    header = 'From: {} \n'.format(mail_server['username'])
    header += 'To: {} \n'.format(', '.join(recipients))
    header += 'Subject: {} \n \n'.format(subject)
    msg = header + body

    mail_stat = email_util.send_mail(mail_server, recipients, msg)

    return mail_stat


def healthcheck():

    res = {
        'msg': 'OK',
        'status': 200,
    }
    return jsonify(res)


def prefix():

    res = {
        'msg': 'No message available',
        'status': 404
    }
    return jsonify(res)


def favicon():
    return '', 204


class StandaloneApplication(gunicorn.app.base.Application):

    def __init__(self, app, config_file):
        self.config_file = config_file
        self.application = app
        super(StandaloneApplication, self).__init__()

    def load_config(self):
        self.load_config_from_file(self.config_file)
        self.cfg.set('worker_class', 'sync')

    def load(self):
        return self.application
