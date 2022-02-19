from airflow import DAG
import pendulum
from airflow.operators.bash_operator import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook


python_extract = SSHHook(remote_host='', 
                         username='', 
                         key_file='')

spark_submit = SSHHook(remote_host='', 
                       username='', 
                       key_file='')
args = {
    'owner': 'Everthon Reis',
}

dag = DAG(
    dag_id='anp_sales_fuel_etl_pipeline',
    default_args=args,
    schedule_interval='00 01 * * *',
    start_date=pendulum.datetime(year=2022, month=2, day=18, tz='America/Sao_Paulo'),
    catchup=False,
    tags=['anp'],
)

hello = BashOperator(
    task_id='start-workflow',
    bash_command="echo 'Starting my Anp Sales Fuel workflow'",
    dag=dag,
)

load_extract_anp = SSHOperator(
    task_id='load_extract_anp',
    command=""" export PYTHONPATH=$PYTHONPATH:/home/ubuntu/data_engineering; 
                echo $PYTHONPATH; 
                cd /home/ubuntu/data_engineering/extract;
                python3.8 anp_fuel_sales_extract.py
            """,
    ssh_hook = python_extract,
    dag=dag,
)

load_transform_sales_of_oil_derivative = SSHOperator(
    task_id='load_transform_sales_of_oil_derivative',
    command="""
            spark-submit --master yarn --deploy-mode client --executor-memory 4g --num-executors 1 --conf spark.dynamicAllocation.enabled=false /home/hadoop/data_engineering/transform/anp_sales_of_oil_derivative_fuels.py; 
            """,
    ssh_hook = spark_submit,
    dag=dag,
)

load_transform_sales_of_diesel = SSHOperator(
    task_id='load_transform_sales_of_diesel',
    command="""
            spark-submit --master yarn --deploy-mode client --executor-memory 4g --num-executors 1 --conf spark.dynamicAllocation.enabled=false /home/hadoop/data_engineering/transform/anp_sales_of_diesel_transform.py; 
            """,
    ssh_hook = spark_submit,
    dag=dag,
)

hello >> load_extract_anp
load_extract_anp >> load_transform_sales_of_oil_derivative
load_transform_sales_of_oil_derivative >> load_transform_sales_of_diesel
