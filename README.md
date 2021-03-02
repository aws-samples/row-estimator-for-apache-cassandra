# Cassandra Row Size Estimator

The Cassandra Row Estimator gathers row level statistics from a Cassandra cluster to help determine roughly a single Cassandra row in bytes to estimate  Read and Write Capacity Unit consumption for Amazon Keyspaces (https://aws.amazon.com/keyspaces/).

When estimating capacity utilization of migrating existing Apache Cassandra workloads to Amazon Keyspaces its important to understand the byte size of a single Cassandra row. With a good understanding of row size, read and write throughput, and raw data volume, one can improve the accuracy of estimating (https://aws.amazon.com/keyspaces/pricing/) the monthly billing for the workload on Amazon Keyspaces “Total Cost of Ownership of a Cassandra” may compromise of many tangible and intangible expenses. 

The challenge customers face in estimating capacity utilization is gathering row level statistics on their existing Cassandra cluster. While some table and partition level information is available through native tooling, Apache Cassandra does not capture row-level metrics. Additionally, there is no current tooling available through the network of the larger Apache Cassandra Community that is able to gather row level metrics.

The Cassandra Row Estimator solves this challenge by providing a way to gather row level statistics from the existing Cassandra cluster. This tool is designed to be run against production and UAT environments in a secure and non-invasive way. No customer data is captured, logged or stored in the process of gathering row metrics. The outputs of this library are byte size statistics of rows distributed across an Apache Cassandra table. 

## Installation

You can install the Cassandra Row Estimator from [PyPI](https://pypi.org/project/cassandra-row-estimator/):

```
$ pip install cassandra-row-estimator
```

The cassandra-row-estimator is supported on Python 3.4 and above.

## How to use

The Cassandra Row Estimator is a command line application, named `cassandra-row-estimator`. To see a list of available options simply call the program:

```
$ cassandra-row-estimator

usage: cassandra-row-estimator [-h] --hostname HOSTNAME --port PORT [--ssl SSL] [--path-cert PATH_CERT] [--username USERNAME] [--password PASSWORD] --keyspace KEYSPACE --table TABLE [--execution-timeout EXECUTION_TIMEOUT] [--token-step TOKEN_STEP]
                               [--rows-per-request ROWS_PER_REQUEST] [--pagination PAGINATION] [--dc DC] [--json JSON]

The tool helps to gather Cassandra rows stats

optional arguments:
  -h, --help            show this help message and exit
  --ssl SSL             Use SSL.
  --path-cert PATH_CERT
                        Path to the TLS certificate
  --username USERNAME   Authenticate as user
  --password PASSWORD   Authenticate using password
  --execution-timeout EXECUTION_TIMEOUT
                        Set execution timeout in seconds
  --token-step TOKEN_STEP
                        Set token step, for example, 2, 4, 8, 16, 32, ..., 255
  --rows-per-request ROWS_PER_REQUEST
                        How many rows per token
  --pagination PAGINATION
                        Turn on pagination mechanism
  --dc DC               Define Cassandra datacenter for routing policy
  --json JSON           Estimata size of Cassandra rows as JSON

required named arguments:
  --hostname HOSTNAME   Cassandra endpoint
  --port PORT           Cassandra native transport port
  --keyspace KEYSPACE   Gather stats against provided keyspace
  --table TABLE         Gather stats against provided table

```

To estimate the row size in the Cassandra cluster, call the program with parameters:

```
	$ cassandra-row-estimator --hostname 0.0.0.0 --port 9042 --username cassandra --password cassandra --keyspace system --table size_estimates --token-step 1 --dc datacenter1 --rows-per-request 1000 
```

## List of Safe Guards

    * Partial range scan based on cluster token ring
    * Paginate results to avoid exhausting cluster connections
    * Manually limit result set to avoid returning large partitions
    * Explicit Query Timeout
    * Explicit timeout of the entire program. Defaults is 3 minutes when running command line
    * LOCAL_ONE consistency for minimum coordinator activity
    * TokenAware load balancing policy reduce network hops

Enjoy! Feedback and PR's welcome!

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
