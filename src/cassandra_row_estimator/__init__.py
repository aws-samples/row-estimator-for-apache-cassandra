#!/usr/bin/env python
# mode: Python
# author: kolesnn@amazon.com
# Usage: The tool helps to estimate row size in C* without impacting cluster performance 
# 

from __future__ import print_function
from sys import getsizeof, stderr
import sys
from itertools import chain
from collections import deque
from ssl import SSLContext, PROTOCOL_TLSv1_2
import math
import operator
from functools import reduce
import optparse
from threading import Thread, Event
import time
import os
import platform
from datetime import datetime
from glob import glob
import warnings

class Estimator(object):
    """ The estimator class containes connetion, stats methods """
    def __init__(self, endpoint_name, port='9042', username=None, password=None, ssl=None, dc=None, keyspace=None, 
                 table=None, execution_timeout=None, token_step=None, rows_per_request=None, pagination=5000):
        self.endpoint_name = endpoint_name
        self.port = port
        self.username = username
        self.password = password
        self.ssl = ssl
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
            ssl_options = {
                'ca_certs' : self.ssl,
                'ssl_version' : PROTOCOL_TLSv1_2
            }
        else:
            ssl_options=None      
  
        if (self.username and self.password):
            auth_provider = PlainTextAuthProvider(self.username, self.password)
  
        cluster = Cluster([self.endpoint_name],port=self.port ,auth_provider=auth_provider, load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=self.dc), ssl_options=ssl_options)
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
        """ The method iterates through ResultSets and returns list of values, where each value approximate footprint of string in memory """
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
                print("INFO: no more token pairs")
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
                        print(row.json)
                        s1 = str(row.json).replace('null','""')
                        rows_bytes.append(self.total_size(s1, verbose=False))
                        if stop_event.is_set():
                            break
            except StopIteration:
                print("INFO: no more token pairs")
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

    def print_out_json(self):
        print("INFO:",datetime.now(),"Number of sampled rows:", len(self.rows_in_bytes))
        print("INFO:",datetime.now(),"*** Estimated size of a Cassandra JSON row")
        print("INFO:",datetime.now(),"Mean:", '{:06.2f}'.format(self.mean(self.rows_in_bytes)),"B")
        print("INFO:",datetime.now(),"Weighted_mean:", '{:06.2f}'.format(self.weighted_mean(self.rows_in_bytes)),"B")
        print("INFO:",datetime.now(),"Median:", '{:06.2f}'.format(self.median(self.rows_in_bytes)),"B") 
        print("INFO:",datetime.now(),"Min:",min(self.rows_in_bytes),"B")
        print("INFO:",datetime.now(),"Max:",max(self.rows_in_bytes),"B")
        print("INFO:",datetime.now(),"Average:",'{:06.2f}'.format(sum(self.rows_in_bytes)/len(self.rows_in_bytes)),"B")
                
def add_helper(n):
    return columns_in_bytes+n       

if __name__ == '__main__':
    # Define version of python enviroment to correctly set UTF encoding
    DEFAULT_PLATFORM=platform.python_version()

    if DEFAULT_PLATFORM[0] == '2':
        reload(sys)
        sys.setdefaultencoding('utf8')

    # Event object used to send signals from one thread to another
    stop_event = Event()
    
    # Configure app args 
    description = 'MCS migration tool helps to gather stats about rows in Apache Cassandra cluster'
    version = '0.01'
    epilog = """ Connects to %('0.0.0.0')s:%('9042')d by default """ 
    parser = optparse.OptionParser(description=description, epilog=epilog, usage="Usage: %prog [options] [host [port]]",version='mcsmtool' + version)
    parser.add_option('--ssl', help='Use SSL.', default=False)
    parser.add_option('--username', help='Authenticate as user.')
    parser.add_option('--password',help='Authenticate using password.')
    parser.add_option('--keyspace',help='Gather stats against provided keyspace.', default='keyspace2')
    parser.add_option('--table',help='Gather stats against provided table.', default='air_routes')
    parser.add_option('--execution-timeout', help='Set execution timeout in seconds', default='360')
    parser.add_option('--token-step', help='Set token step, for example, 2, 4, 8, 16, 32, ..., 255.', default='4')
    parser.add_option('--rows-per-request', help='How many rows per token.', default='10')
    parser.add_option('--pagination', help='Turn on pagination mechanism.', default='15')
    parser.add_option('--dc', help='Define Cassandra datacenter for routing policy', default='datacenter1')
    parser.add_option('--path-to-cassandra', help='Path to cassandra zip libs', default='/usr/share/cassandra/lib')
    parser.add_option('--json', help='Estimata size of Cassandra rows as JSON', default=False)
    optvalues = optparse.Values()
    (options, arguments) = parser.parse_args(sys.argv[1:], values=optvalues)
    
    if len(sys.argv[1:])<=2:
        parser.print_help()
        sys.exit()

    if '--username' in sys.argv[1:]:
        p_username=options.username
    else:
        p_username=None  
    if '--password' in sys.argv[1:]:
        p_password=options.password
    else:
        p_password=None    
    if '--ssl' in sys.argv[1:]:
        p_ssl=options.ssl
    else:
        p_ssl=None
    if '--json' in sys.argv[1:]:
        p_json=options.json
    else:
        p_json=None
    if '--dc' in sys.argv[1:]:
        p_dc=options.dc        
    else:
        p_dc=None
    if '--keyspace' in sys.argv[1:]:  
        p_keyspace=options.keyspace
    else:
        p_keyspace=None
    if '--table' in sys.argv[1:]:
        p_table=options.table
    else:
        p_table=None
    if '--execution-timeout' in sys.argv[1:]:
        p_execution_timeout=int(options.execution_timeout)
    else:
        p_execution_timeout=360
    if '--token-step' in sys.argv[1:]:
        p_token_step=int(options.token_step)
    else:
        p_token_step=None
    if '--rows-per-request' in sys.argv[1:]:
        p_rows_per_request=options.rows_per_request
    else:
        p_rows_per_request=1000
    if '--pagination' in sys.argv[1:]:
        p_pagination=int(options.pagination)
    else:
        p_pagination=5000
    if '--path-to-cassandra' in sys.argv[1:]:
        """ Import Cassandra driver from zip file if Cassandra binary pre-installed """
        ZIPLIB_DIRS=[]
        CQL_LIB_PREFIX='cassandra-driver-internal-only-'

        def find_zip(libprefix):
            ZIPLIB_DIRS.append(options.path_to_cassandra)
            for ziplibdir in ZIPLIB_DIRS:
                zips = glob(os.path.join(ziplibdir, libprefix + '*.zip'))
                if zips:
                    return max(zips)   

        cql_zip = find_zip(CQL_LIB_PREFIX)
        if cql_zip:
            ver = os.path.splitext(os.path.basename(cql_zip))[0][len(CQL_LIB_PREFIX):]
            sys.path.insert(0, os.path.join(cql_zip, 'cassandra-driver-' + ver))

        third_parties = ('futures-', 'six-')

        for lib in third_parties:
            lib_zip = find_zip(lib)
            if lib_zip:
	        sys.path.insert(0, lib_zip)

	    warnings.filterwarnings("ignore", r".*blist.*")    
        
        try:
            from cassandra.cluster import Cluster, Session
            from cassandra.auth import PlainTextAuthProvider
            from cassandra.policies import DCAwareRoundRobinPolicy
            from cassandra import ConsistencyLevel
        except ImportError:
                 sys.exit("Cassandra-driver is not installed")
    else:
        from cassandra.cluster import Cluster, Session
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.policies import DCAwareRoundRobinPolicy
        from cassandra import ConsistencyLevel      
    
    
        estimator = Estimator(arguments[0], arguments[1], username=p_username, password=p_password, ssl=p_ssl, dc=p_dc, keyspace=p_keyspace, table=p_table, 
                          execution_timeout=p_execution_timeout, token_step=p_token_step, rows_per_request=p_rows_per_request, pagination=p_pagination)

        print("INFO:",datetime.now(),"Endpoint:", arguments[0], arguments[1])           
        print("INFO:",datetime.now(),"Keyspace name:", estimator.keyspace)
        print("INFO:",datetime.now(),"Table name:", estimator.table)
        print("INFO:",datetime.now(),"Client SSL:", estimator.ssl)
        print("INFO:",datetime.now(),"Token step:", estimator.token_step)
        print("INFO:",datetime.now(),"Limit of rows per token step:", estimator.rows_per_request)
        print("INFO:",datetime.now(),"Pagination:", estimator.pagination)
        print("INFO:",datetime.now(),"Execution-timeout:", estimator.execution_timeout)        

    if p_json == None:
        action_thread = Thread(target=estimator.row_sampler())
        action_thread.start()
        action_thread.join(timeout=estimator.execution_timeout)
        stop_event.set()
    
        columns_in_bytes = estimator.get_total_column_size()

        rows_in_bytes = estimator.rows_in_bytes
        rows_columns_in_bytes = map(add_helper, rows_in_bytes)    
    
        print("INFO:",datetime.now(),"Number of sampled rows:", len(rows_in_bytes))
        print("INFO:",datetime.now(),"*** Estimated size of column names and values in a row")
        print("INFO:",datetime.now(),"Mean:", '{:06.2f}'.format(estimator.mean(rows_columns_in_bytes)),"B")
        print("INFO:",datetime.now(),"Weighted_mean:", '{:06.2f}'.format(estimator.weighted_mean(rows_columns_in_bytes)),"B")
        print("INFO:",datetime.now(),"Median:", '{:06.2f}'.format(estimator.median(rows_columns_in_bytes)),"B") 
        print("INFO:",datetime.now(),"Min:",min(rows_columns_in_bytes),"B")
        print("INFO:",datetime.now(),"Max:",max(rows_columns_in_bytes),"B")
        print("INFO:",datetime.now(),"Average:",'{:06.2f}'.format(sum(rows_columns_in_bytes)/len(rows_columns_in_bytes)),"B")
        print("INFO:",datetime.now(),"*** Estimated size of values in a row")
        print("INFO:",datetime.now(),"Mean:", '{:06.2f}'.format(estimator.mean(rows_in_bytes)),"B")
        print("INFO:",datetime.now(),"Weighted_mean:", '{:06.2f}'.format(estimator.weighted_mean(rows_in_bytes)),"B")
        print("INFO:",datetime.now(),"Median:", '{:06.2f}'.format(estimator.median(rows_in_bytes)),"B") 
        print("INFO:",datetime.now(),"Min:",min(rows_in_bytes),"B")
        print("INFO:",datetime.now(),"Max:",max(rows_in_bytes),"B")
        print("INFO:",datetime.now(),"Average:",'{:06.2f}'.format(sum(rows_in_bytes)/len(rows_in_bytes)),"B")
        print("INFO:",datetime.now(),"Total column name size in a row:",columns_in_bytes,"B")
        print("INFO:",datetime.now(),"Total column in a row:", estimator.get_columns())
#        print("INFO:",datetime.now(),"Approx. RCUs:",(sum(rows_in_bytes)/len(rows_in_bytes))/4K, celling 4K, amount of rows, "How many rows will fit in a single RCU")
#        print("INFO:",datetime.now(),"Approx. WCUs:", (sum(rows_in_bytes)/len(rows_in_bytes))/1K, celling 1K, amount of rows, "How many rows will git in a single WCU") 
    else:

        action_thread = Thread(target=estimator.row_sampler_json())
        action_thread.start()
        action_thread.join(timeout=estimator.execution_timeout)
        stop_event.set()
        
        estimator.print_out_json()
