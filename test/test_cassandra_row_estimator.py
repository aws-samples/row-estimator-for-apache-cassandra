import pytest

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.cluster import ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy

from cassandra_row_estimator.estimator import Estimator

def add_helper(n):
        return columns_in_bytes+n

@pytest.fixture
def test_cassandra_row_estimator_init():
    endpoint_name='127.0.0.1'
    port=9042
    auth_provider = None
    ssl_context=None
    auth_provider = PlainTextAuthProvider('cassandra', 'cassandra')
    node1_profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([endpoint_name]))
    profiles = {'node1': node1_profile}
    cluster = Cluster([endpoint_name], port=port ,auth_provider=auth_provider, ssl_context=ssl_context, control_connection_timeout=360, execution_profiles=profiles)
    session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS cassandra_row_estimator WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS cassandra_row_estimator.test (a int PRIMARY KEY, b text)")
    params = [1, 'This is a simple test']
    session.execute("INSERT INTO cassandra_row_estimator.test (a, b) VALUES (%s, %s)", params)


estimator = Estimator('127.0.0.1', 9042, 'cassandra', 'cassandra', None, 'datacenter1', 'cassandra_row_estimator', 'test',3600, 2, 1000, 3000, None)
estimator.row_sampler(json=False)
columns_in_bytes = estimator.get_total_column_size()
rows_in_bytes = estimator.rows_in_bytes
rows_columns_in_bytes = map(add_helper, rows_in_bytes)
val = list(rows_columns_in_bytes)

def test_cassandra_row_estimator_row_in_bytes():
    assert rows_in_bytes[0] == 22

def test_cassandra_row_estimator_columns_in_bytes():
    assert columns_in_bytes == 2

def test_cassandra_row_estimator_mean():
    assert estimator.mean(val) == 24   

def test_cassandra_row_estimator_weighted_mean():
    assert estimator.weighted_mean(val) == 24

def test_cassandra_row_estimator_median():
    assert estimator.median(val) == 24

def test_cassandra_row_estimator_qtl_p10():
    assert estimator.quartiles(val, 0.1) == 24

def test_cassandra_row_estimator_qtl_p50():
    assert estimator.quartiles(val, 0.5) == 24

def test_cassandra_row_estimator_qtl_p90():
    assert estimator.quartiles(val, 0.9) == 24
