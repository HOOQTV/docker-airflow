from airflow.models import DAG
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

from takia_production_logs.utils.queries import CREATE_STAGE, DROP_STAGE, ADD_PARTITION
from takia_production_logs.utils.bash_commands import *

with DAG(dag_id='takia_production_logs',
         template_searchpath='/Users/tingyao/airflow/takia_production_logs/sql/',
         schedule_interval='@daily',
         start_date=datetime(2019, 7, 8)) as dag:

    parameters = {
        'dt': '{{ execution_date.strftime("%Y%m%d") }}',
        'event_date': '{{ execution_date.strftime("%Y-%m-%d") }}',
        'day': '{{ execution_date.strftime("%d") }}',
        'month': '{{ execution_date.strftime("%m") }}',
        'year': '{{ execution_date.strftime("%Y") }}'
    }

    output_location = 's3://hooq-takia-production-logs/query_output/'

    drop_staging_table = AWSAthenaOperator(
        task_id='drop_staging_table',
        query=DROP_STAGE.format(**parameters),
        output_location=output_location,
        database='dw_stage',
        aws_conn_id='tingyao_aws',
        sleep_time=5
    )

    create_staging_table = AWSAthenaOperator(
        task_id='create_staging_table',
        query=CREATE_STAGE.format(**parameters),
        output_location=output_location,
        database='dw_stage',
        aws_conn_id='tingyao_aws',
        sleep_time=5
    )

    s3_backup_prod = BashOperator(
        task_id='s3_backup_prod',
        bash_command=S3_BACKUP.format(**parameters),
        dag=dag)

    s3_insert_partitions_into_prod = BashOperator(
        task_id='s3_insert_partitions_into_prod',
        bash_command=S3_INSERT.format(**parameters),
        dag=dag)

    s3_delete_partitions_if_success = BashOperator(
        task_id='s3_delete_partitions_if_success',
        bash_command=S3_DELETE.format(**parameters),
        dag=dag)

    s3_restore_if_failure = BashOperator(
        task_id='s3_restore_if_failure',
        bash_command=S3_RESTORE.format(**parameters),
        trigger_rule='one_failed',
        dag=dag)

    repartition_prod = AWSAthenaOperator(
        task_id='repartition_prod',
        query=ADD_PARTITION.format(**parameters),
        output_location=output_location,
        database='dw_prod',
        aws_conn_id='tingyao_aws',
        trigger_rule='all_done'
    )

    drop_staging_table >> create_staging_table
    create_staging_table >> s3_backup_prod >> s3_insert_partitions_into_prod
    s3_insert_partitions_into_prod >> (s3_delete_partitions_if_success, s3_restore_if_failure) >> repartition_prod
