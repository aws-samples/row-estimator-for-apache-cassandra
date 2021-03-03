import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
     name = "cassandra_row_estimator",
     packages = ["cassandra_row_estimator"],
     include_package_data=True,
     version = "0.0.1",
     description = "The Row Estimator for Apache Cassandra gathers row level statistics from a Cassandra cluster to help determine roughly a single Cassandra row in bytes to estimate",
     author = "kolesnn",
     author_email = "kolesnn@amazon.com",
     url = "https://github.com/aws-samples/row-estimator-for-cassandra",
     download_url = "https://github.com/awslabs/row-estimator-for-cassandra/cassandra_row_estimator-v0.1.tgz",
     keywords = "cassandra_row_estimator Cassandra Keyspaces CassandraRow",
     license='Apache License 2.0',
     classifiers=[
           "Development Status :: 4 - Beta", 
           "Topic :: Utilities",
           "License :: OSI Approved :: Apache Software License",
       ],
     python_requires='>=3.6',
     install_requires=[
        "cassandra-driver"
     ],
    entry_points={
        "console_scripts": [
            "cassandra-row-estimator=cassandra_row_estimator.__main__:main",
        ]
    },
   )
