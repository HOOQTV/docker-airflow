#!/usr/bin/env python3
import boto3
# Function for executing athena queries


def run_query(query, database, s3_output):
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        }
    )
    print('Execution ID: ' + response['QueryExecutionId'])
    return response


# Athena configuration
s3_input = 's3://athena-how-to/data'
s3_ouput = 's3://athena-how-to/results/'
database = 'test_database'
table = 'persons'
# Athena database and table definition
create_database = "CREATE DATABASE IF NOT EXISTS %s;" % (database)
create_table = \
    """CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    `name` string,
    `sex`string,
    `city` string,
    `country` string,
    `age` int,
    `job` string
     )
     ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
     WITH SERDEPROPERTIES (
     'serialization.format' = '1'
     ) LOCATION '%s'
     TBLPROPERTIES ('has_encrypted_data'='false');""" % (database, table, s3_input)
# Query definitions
query_1 = "SELECT * FROM %s.%s where sex = 'F';" % (database, table)
query_2 = "SELECT * FROM %s.%s where age > 30;" % (database, table)
# Execute all queries
queries = [create_database, create_table, query_1, query_2]
for q in queries:
    print("Executing query: %s" % (q))
    res = run_query(q, database, s3_ouput)
