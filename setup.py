import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
     name = "row-estimator-for-apache-cassandra",
     packages = ["row_estimator_for_apache_cassandra"],
     include_package_data=True,
     version = "0.0.3",
     description = "The Row Estimator for Apache Cassandra gathers row level statistics from a Cassandra cluster to help determine roughly a single Cassandra row in bytes to estimate",
     long_description=long_description,
     long_description_content_type="text/markdown",
     author = "kolesnn",
     author_email = "kolesnn@amazon.com",
     url = "https://github.com/aws-samples/row-estimator-for-apache-cassandra",
     download_url = "https://github.com/aws-samples/row-estimator-for-apache-cassandra/row_estimator_for_apache_cassandra-v0.1.tgz",
     keywords = "row_estimator_for_apache_cassandra ApacheCassandra AmazonKeyspaces",
     license='Apache License 2.0',
     classifiers=[
           "Development Status :: 5 - Production/Stable", 
           "Topic :: Utilities",
           "License :: OSI Approved :: Apache Software License",
       ],
     python_requires='>=3.6',
     install_requires=[
        "cassandra-driver"
     ],
    entry_points={
        "console_scripts": [
            "cassandra-row-estimator=row_estimator_for_apache_cassandra.__main__:main",
        ]
    },
   )
