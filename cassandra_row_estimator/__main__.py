#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
  
#Licensed under the Apache License, Version 2.0 (the "License").
#You may not use this file except in compliance with the License.
#You may obtain a copy of the License at
  
#     http://www.apache.org/licenses/LICENSE-2.0
  
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

from importlib import resources  # Python 3.7+
import sys
import logging
import argparse

from threading import Thread, Event

from cassandra_row_estimator.estimator import Estimator

def main():
    def add_helper(n):
        return columns_in_bytes+n

    logging.getLogger('cassandra').setLevel(logging.ERROR)
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    # Event object used to send signals from one thread to another
    stop_event = Event()

    # Configure app args
    parser = argparse.ArgumentParser(description='The tool helps to gather Cassandra rows stats')
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('--hostname', help='Cassandra endpoint', default='127.0.0.1', required=True)
    requiredNamed.add_argument('--port', help='Cassandra native transport port', required=True)
    parser.add_argument('--ssl', help='Use SSL.', default=None)
    parser.add_argument('--path-cert', help='Path to the TLS certificate', default=None)
    parser.add_argument('--username', help='Authenticate as user')
    parser.add_argument('--password',help='Authenticate using password')
    requiredNamed.add_argument('--keyspace',help='Gather stats against provided keyspace', required=True)
    requiredNamed.add_argument('--table',help='Gather stats against provided table', required=True)
    parser.add_argument('--execution-timeout', help='Set execution timeout in seconds', type=int, default=360)
    parser.add_argument('--token-step', help='Set token step, for example, 2, 4, 8, 16, 32, ..., 255',type=int, default=4)
    parser.add_argument('--rows-per-request', help='How many rows per token',type=int, default=1000)
    parser.add_argument('--pagination', help='Turn on pagination mechanism',type=int, default=200)
    parser.add_argument('--dc', help='Define Cassandra datacenter for routing policy', default='datacenter1')
    parser.add_argument('--json', help='Estimata size of Cassandra rows as JSON', default=None)
    
    if (len(sys.argv)<2):
        parser.print_help()
        sys.exit()

    args = parser.parse_args()
    p_hostname = args.hostname
    p_port = args.port
    p_username = args.username
    p_password = args.password
    p_ssl = args.ssl
    p_path_cert = args.path_cert
    p_json = args.json
    p_dc = args.dc
    p_keyspace = args.keyspace
    p_table = args.table
    p_execution_timeout = args.execution_timeout 
    p_token_step = args.token_step
    p_rows_per_request = args.rows_per_request
    p_pagination = args.pagination  
    
    estimator = Estimator(p_hostname, p_port, p_username, p_password, p_ssl, p_dc, p_keyspace, p_table,
                          p_execution_timeout, p_token_step, p_rows_per_request, p_pagination, p_path_cert)

    logging.info("Endpoint: %s %s", p_hostname, p_port)
    logging.info("Keyspace name: %s", estimator.keyspace)
    logging.info("Table name: %s", estimator.table)
    logging.info("Client SSL: %s", estimator.ssl)
    logging.info("Token step: %s", estimator.token_step)
    logging.info("Limit of rows per token step: %s", estimator.rows_per_request)
    logging.info("Pagination: %s", estimator.pagination)
    logging.info("Execution-timeout: %s", estimator.execution_timeout)

    if p_json == None:
        action_thread = Thread(target=estimator.row_sampler(json=False))
        action_thread.start()
        action_thread.join(timeout=estimator.execution_timeout)
        stop_event.set()

        columns_in_bytes = estimator.get_total_column_size()

        rows_in_bytes = estimator.rows_in_bytes
        rows_columns_in_bytes = map(add_helper, rows_in_bytes)
        val = list(rows_columns_in_bytes)
        logging.info("Number of sampled rows: %s", len(rows_in_bytes))
        logging.info("Estimated size of column names and values in a row:")
        logging.info("	Mean: %s", '{:06.2f}'.format(estimator.mean(val)))
        logging.info("	Weighted_mean: %s", '{:06.2f}'.format(estimator.weighted_mean(val)))
        logging.info("	Median: %s", '{:06.2f}'.format(estimator.median(val)))
        logging.info("	Min: %s",min(val))
        logging.info("	P10: %s",'{:06.2f}'.format(estimator.quartiles(val, 0.1)))
        logging.info("	P50: %s",'{:06.2f}'.format(estimator.quartiles(val, 0.5)))
        logging.info("	P90: %s",'{:06.2f}'.format(estimator.quartiles(val, 0.9)))
        logging.info("	Max: %s",max(val))
        logging.info("	Average: %s",'{:06.2f}'.format(sum(val)/len(val)))
        logging.info("Estimated size of values in a row")
        logging.info("	Mean: %s", '{:06.2f}'.format(estimator.mean(rows_in_bytes)))
        logging.info("	Weighted_mean: %s", '{:06.2f}'.format(estimator.weighted_mean(rows_in_bytes)))
        logging.info("	Median: %s", '{:06.2f}'.format(estimator.median(rows_in_bytes)))
        logging.info("	Min: %s",min(rows_in_bytes))
        logging.info("	P10: %s",'{:06.2f}'.format(estimator.quartiles(rows_in_bytes, 0.1)))
        logging.info("	P50: %s",'{:06.2f}'.format(estimator.quartiles(rows_in_bytes, 0.5)))
        logging.info("	P90: %s",'{:06.2f}'.format(estimator.quartiles(rows_in_bytes, 0.9)))
        logging.info("	Max: %s",max(rows_in_bytes))
        logging.info("	Average: %s",'{:06.2f}'.format(sum(rows_in_bytes)/len(rows_in_bytes)))
        logging.info("Total column name size in a row: %s",columns_in_bytes)
        logging.info("Columns in a row: %s", estimator.get_columns().count(',')+1)
    else:
        action_thread = Thread(target=estimator.row_sampler(json=True))
        action_thread.start()
        action_thread.join(timeout=estimator.execution_timeout)
        stop_event.set()
        rows_in_bytes = estimator.rows_in_bytes
        logging.info("Number of sampled rows: %s", len(rows_in_bytes))
        logging.info("Estimated size of a Cassandra JSON row")
        logging.info("Mean: %s", '{:06.2f}'.format(estimator.mean(rows_in_bytes)))
        logging.info("Weighted_mean: %s", '{:06.2f}'.format(estimator.weighted_mean(rows_in_bytes)))
        logging.info("Median: %s", '{:06.2f}'.format(estimator.median(rows_in_bytes)))
        logging.info("Min: %s",min(rows_in_bytes))
        logging.info("Max: %s",max(rows_in_bytes))
        logging.info("Average: %s",'{:06.2f}'.format(sum(rows_in_bytes)/len(rows_in_bytes)))

if __name__ == "__main__":
    main()
