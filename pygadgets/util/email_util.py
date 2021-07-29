#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Jul 10 15:48:56 2019

@author: david
"""
import smtplib

from . import db_util


def send_mail(mail_server, recipients, msg):

    try:
        if recipients:
            server = smtplib.SMTP(mail_server['host'], port=587, timeout=10)
            server.starttls()
            server.login(mail_server['username'], mail_server['password'])
            server.sendmail(mail_server['username'], recipients, msg)
            server.quit()

            return 'Mail Sent.'

        else:
            return 'No Recipient.'

    except:
        return 'Mail Sending Error.'
