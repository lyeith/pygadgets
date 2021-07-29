#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Jun 09 11:46:08 2020

@author: david
"""
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.policies import RetryPolicy
from cassandra.query import dict_factory