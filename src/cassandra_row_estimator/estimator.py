from sys import getsizeof, stderr
import sys

from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.cluster import ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy

import math
from functools import reduce
import optparse
import time
import os
from datetime import datetime

from threading import Thread, Event

from collections import deque

stop_event = Event()

class Estimator(object):
    
    """ The estimator class containes connetion, stats methods """
    def __init__(self, endpoint_name, port='9042', username=None, password=None, ssl=None, dc=None, keyspace=None,
                 table=None, execution_timeout=None, token_step=None, rows_per_request=None, pagination=5000, path_cert=None):
        self.endpoint_name = endpoint_name
        self.port = port
        self.username = username
        self.password = password
        self.ssl = ssl
        self.path_cert = path_cert
        self.keyspace = keyspace
        self.table = table
        self.execution_timeout = execution_timeout
        self.token_step = token_step
        self.rows_per_request = rows_per_request
        self.pagination = pagination
        self.dc = dc
        self.rows_in_bytes = []
    
    def get_connection(self):
        """ Returns Cassandra session """
        auth_provider = None

        if self.ssl:
            ssl_context = SSLContext(PROTOCOL_TLSv1)
            ssl_context.load_verify_locations(path_cert)
            ssl_context.verify_mode = CERT_REQUIRED
            #ssl_options = {
            #    'ca_certs' : self.ssl,
            #    'ssl_version' : PROTOCOL_TLSv1_2
            #}
        else:
            ssl_context=None

        if (self.username and self.password):
            auth_provider = PlainTextAuthProvider(self.username, self.password)

        node1_profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([self.endpoint_name]))
        profiles = {'node1': node1_profile}
        cluster = Cluster([self.endpoint_name], port=self.port ,auth_provider=auth_provider,  ssl_context=ssl_context, control_connection_timeout=360, execution_profiles=profiles)
        session = cluster.connect()
        return session

    def mean(self, list):
        """ Calculate the mean of list of Cassandra values """
        final_mean = 0.0

        for n in list:
            final_mean += n
        final_mean = final_mean / float(len(list))
        return final_mean

    def weighted_mean(self, list):
        """ Calculates the weighted mean of a list of Cassandra values """

        total = 0
        total_weight = 0
        normalized_weights = []

        # Set up some lists for our weighted values, and weighted means
        weights = [1 + n for n in range(len(list))]
        normalized_weights = [0 for n in range(len(list))]

        # Calculate a total of all weights
        total_weight = reduce(lambda y,x: x+y, weights)

        # Divide each weight by the sum of all weights
        for q,r in enumerate(weights):
            normalized_weights[q] = r / float(total_weight)

        # Add values of original List multipled by weighted values
        for q,r in enumerate(list):
            total +=r * normalized_weights[q]

        return total

    def median(self, list):
        """ Calculate the median of Cassandra values """
        """ The middle value in the set """

        tmp_list = sorted(list)
        index = (len(list) - 1) // 2

        # If the set has an even number of entries, combine the middle two
        # Otherwise print the middle value
        if len(list) % 2 == 0:
            return ((tmp_list[index] + tmp_list[index + 1]) / 2.0)
        else:
            return tmp_list[index]

    def quartiles(self, lst, q):
        """ Quartiles in stats are values that devide your data into quarters """
        lst.sort()
        cnt = len(lst)
        ith = int(round(q*(cnt+1)))
        return lst[ith]

    def total_size(self, obj, handlers={}, verbose=False):
        """
            Gets ~ memory footprint an object and all of its contents.
            Finds the contents of the following builtin containers and
            their subclasses:  tuple, list, deque, dict, set and frozenset.
            To search other containers, add handlers to iterate over their contents:
            handlers = {SomeContainerClass: iter, OtherContainerClass: OtherContainerClass.get_elements}
        """
        dict_handler = lambda d: chain.from_iterable(d.items())
        all_handlers = {tuple: iter,
                    list: iter,
                    deque: iter,
                    dict: dict_handler,
                    set: iter,
                    frozenset: iter,
                   }
        all_handlers.update(handlers)
        seen = set()
        default_size = getsizeof(0)
        obj_empty_size = getsizeof('')

        def sizeof(obj):
            if id(obj) in seen:
                return 0
            seen.add(id(obj))
            s = getsizeof(obj, default_size)

            if verbose:
                print(s, type(obj), repr(obj), file=stderr)

            for typ, handler in all_handlers.items():
                if isinstance(obj, typ):
                    s += sum(map(sizeof, handler(obj)))
                    break
            return s

        return sizeof(obj)-obj_empty_size

    def row_sampler(self):
        """ The method iterates through ResultSets and returns list of values, where each value approximates footprint of string in memory """
        a=0
        session = self.get_connection()
        pk = self.get_partition_key()

        tbl_lookup_stmt = session.prepare("SELECT * FROM "+self.keyspace+"."+self.table+" WHERE token("+pk+")>? AND token("+pk+")<? LIMIT "+str(self.rows_per_request))
        tbl_lookup_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE
        tbl_lookup_stmt.fetch_size=int(self.pagination)

        ring=[]
        ring_values_by_step=[]
        rows_bytes=[]

        for r in session.cluster.metadata.token_map.ring:
            ring.append(r.value)
        for i in ring[::self.token_step]:
            ring_values_by_step.append(i)
        it = iter(ring_values_by_step)

        for val in it:
            try:
                results = session.execute(tbl_lookup_stmt, [val, next(it)])
                for row in results:
                    for value in row:
                        s1 = str(value)
                        a +=self.total_size(s1, verbose=False)
                    rows_bytes.append(a)
                    a = 0
                    if stop_event.is_set():
                        break
            except StopIteration:
                logger.info("no more token pairs")
        self.rows_in_bytes = rows_bytes

    def row_sampler_json(self):
        session = self.get_connection()
        cl = self.get_columns()
        pk = self.get_partition_key()


        tbl_lookup_stmt = session.prepare("SELECT json "+cl+" FROM "+self.keyspace+"."+self.table+" WHERE token("+pk+")>? AND token("+pk+")<? LIMIT "+str(self.rows_per_request))
        tbl_lookup_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE
        tbl_lookup_stmt.fetch_size=int(self.pagination)

        ring=[]
        ring_values_by_step=[]
        rows_bytes=[]

        for r in session.cluster.metadata.token_map.ring:
            ring.append(r.value)
        for i in ring[::self.token_step]:
            ring_values_by_step.append(i)
        it = iter(ring_values_by_step)

        for val in it:
            try:
                results = session.execute(tbl_lookup_stmt, [val, next(it)])
                for row in results:
                        s1 = str(row.json).replace('null','""')
                        rows_bytes.append(self.total_size(s1, verbose=False))
                        if stop_event.is_set():
                            break
            except StopIteration:
                logger.info("no more token pairs")
        self.rows_in_bytes = rows_bytes

    def get_total_column_size(self):
        """ The method returns total size of field names in ResultSets """
        session = self.get_connection()
        columns_results_stmt = session.prepare("select column_name from system_schema.columns where keyspace_name=? and table_name=?")
        columns_results_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE
        columns_results = session.execute(columns_results_stmt, [self.keyspace, self.table])
        clmsum=0
        for col in columns_results:
            clmsum += self.total_size(col.column_name, verbose=False)
        return clmsum

    def get_partition_key(self):
        """ The method returns parition key """
        session = self.get_connection()
        pk_results_stmt = session.prepare("select column_name from system_schema.columns where keyspace_name=? and table_name=? and kind='partition_key' ALLOW FILTERING")
        pk_results_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE
        pk_results = session.execute(pk_results_stmt, [self.keyspace, self.table])
        pk=[]
        for p in pk_results:
            pk.append(p.column_name)
        if len(pk)>1:
            pk_string = ','.join(pk)
        else:
            pk_string = pk[0]
        return pk_string

    def get_columns(self):
        """ The method returns all columns """
        session = self.get_connection()
        columns_results_stmt = session.prepare("select column_name from system_schema.columns where keyspace_name=? and table_name=?")
        columns_results_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE
        columns_results = session.execute(columns_results_stmt, [self.keyspace, self.table])
        clms=[]
        for c in columns_results:
            clms.append(c.column_name)
        if len(clms)>1:
            cl_string = ','.join(clms)
        else:
            cl_string = clms[0]
        return cl_string

