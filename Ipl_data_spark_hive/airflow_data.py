from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitPySparkJobOperator,  # Add this import if using DataprocSubmitPySparkJobOperator
    # DataprocSubmitJobOperator,  # Import for DataprocSubmitJobOperator if using it
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

# all get import
# Define your GCS bucket and paths
bucket = "daily-csv-files"

# Assuming 'date' is defined earlier in your DAG or derived from some other source
date = datetime.now().strftime("%Y%m%d")  # Format the date as needed

# Define the path where the files are located for the given date
object_name = f"orders_{date}.csv"


# create default argument
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# create DAG
dag=DAG(
    'gcp_dataproc_pyspark_dag',
    default_args=default_args,
    description='A DAG to run Spark job on Dataproc',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 28),
    tags=['example'],
)



# Define cluster config
config = Variable.get("cluster_credentials", deserialize_json=True)
CLUSTER_NAME = config['CLUSTER_NAME']
PROJECT_ID = config['PROJECT_ID']
REGION = config['REGION']
#ZONE = 'us-central1-b'
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1,
        'machine_type_uri': 'n1-standard-2',  # Changed machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 50
        }
    },
    'worker_config': {
        'num_instances': 2,  # Reduced the number of worker nodes to 1
        'machine_type_uri': 'n1-standard-2',  # Changed machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 32
        }
    },
    'software_config': {
        'image_version': '2.1-debian11'  # This is an example, please choose a supported version.
    }
}

# create cluster
create_cluster = DataprocCreateClusterOperator(
    task_id='create_cluster',
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    dag=dag,
)

#read pyspark file from path or storage
pyspark_job_file_path = 'gs://daily-csv-files/spark_job_for_project.py'


# Check if execution_date is provided manually, otherwise use the default execution date
# date_variable = "{{ ds_nodash }}"
date_variable = "{{ dag_run.conf['execution_date'] if dag_run and dag_run.conf and 'execution_date' in dag_run.conf else ds_nodash }}"


# Sensor to check for file existence in GCS0
gcs_sensor_task = GCSObjectExistenceSensor(
    task_id='check_csv_file',
    bucket=bucket,  # Specify the bucket variable defined earlier
    object=object_name,  # Specify the object_name variable defined earlier
    mode='poke',
    poke_interval=300,  # Poke every 5 minutes
    timeout=43200,  # Timeout after 12 hours (5 minutes * 144)
    dag=dag,
)


# spark submit
submit_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_job',
    main=pyspark_job_file_path,
    arguments=[f"--date={date_variable}"],  # Passing date as an argument to the PySpark script
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)



# all done now delete auto cluster to save cost
delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',  # ensures cluster deletion even if Spark job fails
    dag=dag,
)


# Define task dependencies
create_cluster >> gcs_sensor_task >> submit_pyspark_job >> delete_cluster













