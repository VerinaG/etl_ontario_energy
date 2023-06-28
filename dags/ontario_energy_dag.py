from datetime import timedelta, datetime

# The DAG object
from airflow import DAG

# Operators
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Modules
from scripts import filename_helper
from scripts import s3_helper
from scripts import transform_helper
from scripts import cleanup_helper

# Get filename info
output_file_name = filename_helper.get_output_file_name()
intertie_file_names, load_file_names, year, month = filename_helper.get_daily_data_file_names()
# Get airflow S3 connection
s3_hook = S3Hook('s3_conn')


# initializing the default arguments
default_args = {
        'owner': 'Ranga',
        'start_date': datetime(2023, 5, 1),
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
ontario_energy_dag = DAG('ontario_energy_dag',
                         default_args=default_args,
                         description='Ontario Energy ETL DAG',
                         schedule='0 0 1 * *',
                         catchup=False,
                         tags=['ontarioenergy'])

# Pull monthly output data and upload to ontarioenergybucket S3 bucket
pull_output_data = PythonOperator(
    task_id='output_data_pull',
    python_callable=s3_helper.pull_data,
    op_kwargs={
        's3_hook': s3_hook,
        'url_prefix': 'http://reports.ieso.ca/public/GenOutputCapabilityMonth/',
        'bucket_name': 'ontarioenergybucket',
        'file_names': output_file_name,
    },
    retry_delay=timedelta(days=1),
    dag=ontario_energy_dag,
)

# Pull import/export data and upload to ontarioenergybucket/intertie S3 bucket
pull_intertie_data = PythonOperator(
    task_id='intertie_data_pull',
    python_callable=s3_helper.pull_data,
    op_kwargs={
        's3_hook': s3_hook,
        'url_prefix': 'http://reports.ieso.ca/public/IntertieScheduleFlow/',
        'bucket_name': 'ontarioenergybucket',
        'file_names': intertie_file_names,
        'path': 'intertieSchedule/'
    },
    retry_delay=timedelta(days=1),
    dag=ontario_energy_dag,
)

# Pull load data and upload to ontarioenergybucket/loadSchedule S3 bucket
pull_load_data = PythonOperator(
    task_id='load_data_pull',
    python_callable=s3_helper.pull_data,
    op_kwargs={
        's3_hook': s3_hook,
        'url_prefix': 'http://reports.ieso.ca/public/DAConstTotals/',
        'bucket_name': 'ontarioenergybucket',
        'file_names': load_file_names,
        'path': 'loadSchedule/'
    },
    retry_delay=timedelta(days=1),
    dag=ontario_energy_dag,
)

# Download output data to local working directory
download_output_data_from_s3 = PythonOperator(
    task_id='download_output_data_from_s3',
    python_callable=s3_helper.download_from_s3,
    op_kwargs={
        's3_hook': s3_hook,
        'bucket_name': 'ontarioenergybucket',
        'file_names': output_file_name,
        'local_path': './data/output'
    },
    dag=ontario_energy_dag
)

# Download intertie data to local working directory
download_intertie_data_from_s3 = PythonOperator(
    task_id='download_intertie_data_from_s3',
    python_callable=s3_helper.download_from_s3,
    op_kwargs={
        's3_hook': s3_hook,
        'bucket_name': 'ontarioenergybucket',
        'file_names': intertie_file_names,
        'path': 'intertieSchedule/',
        'local_path': './data/intertie_load'
    },
    dag=ontario_energy_dag
)

# Download load data to local working directory
download_load_data_from_s3 = PythonOperator(
    task_id='download_load_data_from_s3',
    python_callable=s3_helper.download_from_s3,
    op_kwargs={
        's3_hook': s3_hook,
        'bucket_name': 'ontarioenergybucket',
        'file_names': load_file_names,
        'path': 'loadSchedule/',
        'local_path': './data/intertie_load'
    },
    dag=ontario_energy_dag
)

# Circuit breaker to stop bad output data
output_data_circuit_breaker = ShortCircuitOperator(
    task_id='output_data_circuit_breaker',
    python_callable=transform_helper.check_output_columns,
    op_kwargs={
        'output_file_name': output_file_name,
    },
    dag=ontario_energy_dag
)

# Circuit breaker to stop bad intertie data
intertie_data_circuit_breaker = ShortCircuitOperator(
    task_id='intertie_data_circuit_breaker',
    python_callable=transform_helper.check_intertie_nodes,
    op_kwargs={
        'intertie_file_names': intertie_file_names,
    },
    dag=ontario_energy_dag
)

# Circuit breaker to stop bad load data
load_data_circuit_breaker = ShortCircuitOperator(
    task_id='load_data_circuit_breaker',
    python_callable=transform_helper.check_load_nodes,
    op_kwargs={
        'load_file_names': load_file_names,
    },
    dag=ontario_energy_dag
)

# Transform monthly output data and save to new file
transform_monthly_output_data = PythonOperator(
    task_id='transform_monthly_output_data',
    python_callable=transform_helper.transform_output_data,
    op_kwargs={
        'output_file_name': output_file_name,
    },
    dag=ontario_energy_dag
)

# Transform import/export data and save to new file
transform_intertie_load_data = PythonOperator(
    task_id='transform_intertie_load_data',
    python_callable=transform_helper.transform_import_export_load_data,
    op_kwargs={
        'intertie_file_names': intertie_file_names,
        'load_file_names': load_file_names,
        'year': year,
        'month': month,
    },
    dag=ontario_energy_dag
)

# Upload data to Snowflake
stage_output_to_snowflake = SnowflakeOperator(
    task_id='stage_output_to_snowflake',
    snowflake_conn_id='snowflake_conn',
    sql='PUT file://data/output/{{ params.filename }} @~/{{ params.stage }}',
    params={'filename': 'transformed*.csv',
            'stage': 'staged'},
    dag=ontario_energy_dag
)

# Upload data to Snowflake
stage_intertie_load_to_snowflake = SnowflakeOperator(
    task_id='stage_intertie_load_to_snowflake',
    snowflake_conn_id='snowflake_conn',
    sql='PUT file://data/intertie_load/{{ params.filename }} @~/{{ params.stage }}',
    params={'filename': 'transformed_*.csv',
            'stage': 'staged/intertie_load'},
    dag=ontario_energy_dag
)

# Upload data to Snowflake
copy_output_to_table_snowflake = SnowflakeOperator(
    task_id='copy_output_to_table_snowflake',
    snowflake_conn_id='snowflake_conn',
    sql='COPY INTO HOURLY_ENERGY_CAPABILITY_OUTPUT from @~/staged FILE_FORMAT = csv_format',
    dag=ontario_energy_dag
)

# Upload data to Snowflake
copy_intertie_load_to_table_snowflake = SnowflakeOperator(
    task_id='copy_intertie_load_to_table_snowflake',
    snowflake_conn_id='snowflake_conn',
    sql='COPY INTO HOURLY_IMPORT_EXPORT from @~/staged/intertie_load FILE_FORMAT = csv_format',
    dag=ontario_energy_dag
)

# Upload data to Snowflake
clean_stage_snowflake = SnowflakeOperator(
    task_id='clean_stage_snowflake',
    snowflake_conn_id='snowflake_conn',
    sql='REMOVE @~/staged',
    dag=ontario_energy_dag
)

# Clean local directory
clean_local_directory_task = PythonOperator(
    task_id='clean_local_directory',
    python_callable=cleanup_helper.clean_local_directory,
    dag=ontario_energy_dag
)

# Set the order of execution of tasks
# Pull all data first to ensure resources exist
download_output_data_from_s3.set_upstream([pull_output_data,
                                           pull_intertie_data,
                                           pull_load_data])
download_intertie_data_from_s3.set_upstream([pull_output_data,
                                             pull_intertie_data,
                                             pull_load_data])
download_load_data_from_s3.set_upstream([pull_output_data,
                                         pull_intertie_data,
                                         pull_load_data])
# Output Data
download_output_data_from_s3 >> output_data_circuit_breaker
output_data_circuit_breaker >> transform_monthly_output_data
transform_monthly_output_data >> stage_output_to_snowflake
stage_output_to_snowflake >> copy_output_to_table_snowflake

# Import/Export/Load Data
download_intertie_data_from_s3 >> intertie_data_circuit_breaker
download_load_data_from_s3 >> load_data_circuit_breaker
[intertie_data_circuit_breaker, load_data_circuit_breaker] >> transform_intertie_load_data
transform_intertie_load_data >> stage_intertie_load_to_snowflake
stage_intertie_load_to_snowflake >> copy_intertie_load_to_table_snowflake

# Clean file stores
[copy_output_to_table_snowflake, copy_intertie_load_to_table_snowflake] >> clean_stage_snowflake
clean_stage_snowflake >> clean_local_directory_task
