from importlib import resources  # Python 3.7+
import sys
import logging
import argparse

from threading import Thread, Event

#from cassandra_row_estimator.estimator import Estimator
from estimator import Estimator

def main():
    def add_helper(n):
        return columns_in_bytes+n

    logging.getLogger('cassandra').setLevel(logging.ERROR)
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    # Event object used to send signals from one thread to another
    stop_event = Event()

    # Configure app args
    parser = argparse.ArgumentParser(description='The tool helps to gather Cassandra rows stats')
    parser.add_argument('--hostname', help='Cassandra endpoint', default='127.0.0.1')
    parser.add_argument('--port', help='Cassandra native transport port')
    parser.add_argument('--ssl', help='Use SSL.', default=None)
    parser.add_argument('--path-cert', help='Path to the TLS certificate', default=None)
    parser.add_argument('--username', help='Authenticate as user')
    parser.add_argument('--password',help='Authenticate using password')
    parser.add_argument('--keyspace',help='Gather stats against provided keyspace', default='system')
    parser.add_argument('--table',help='Gather stats against provided table', default='size_estimates')
    parser.add_argument('--execution-timeout', help='Set execution timeout in seconds', type=int, default=360)
    parser.add_argument('--token-step', help='Set token step, for example, 2, 4, 8, 16, 32, ..., 255',type=int, default=4)
    parser.add_argument('--rows-per-request', help='How many rows per token',type=int, default=1000)
    parser.add_argument('--pagination', help='Turn on pagination mechanism',type=int, default=200)
    parser.add_argument('--dc', help='Define Cassandra datacenter for routing policy', default='datacenter1')
    parser.add_argument('--json', help='Estimata size of Cassandra rows as JSON', default=None)
    args = parser.parse_args()

    if len(sys.argv[1:])<2:
        parser.print_help()
        sys.exit()

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
    
    estimator = Estimator(p_hostname, p_port, username=p_username, password=p_password, ssl=p_ssl, dc=p_dc, keyspace=p_keyspace, table=p_table,
                          execution_timeout=p_execution_timeout, token_step=p_token_step, rows_per_request=p_rows_per_request, pagination=p_pagination, path_cert=p_path_cert)

    logging.info("Endpoint: %s %s", p_hostname, p_port)
    logging.info("Keyspace name: %s", estimator.keyspace)
    logging.info("Table name: %s", estimator.table)
    logging.info("Client SSL: %s", estimator.ssl)
    logging.info("Token step: %s", estimator.token_step)
    logging.info("Limit of rows per token step: %s", estimator.rows_per_request)
    logging.info("Pagination: %s", estimator.pagination)
    logging.info("Execution-timeout: %s", estimator.execution_timeout)

    if p_json == None:
        action_thread = Thread(target=estimator.row_sampler())
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
        #logging.info("	Weighted_mean: %s", '{:06.2f}'.format(estimator.weighted_mean(list(rows_columns_in_bytes))))
        logging.info("	Median: "+str(estimator.median(val)))
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
        action_thread = Thread(target=estimator.row_sampler_json())
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
