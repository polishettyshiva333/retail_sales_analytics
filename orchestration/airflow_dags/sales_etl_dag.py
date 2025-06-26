from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
import papermill as pm
from email.mime.text import MIMEText
import smtplib


from datetime import datetime, timedelta
import os
import subprocess
import snowflake.connector
import yaml
from dotenv import load_dotenv

# Ensure the environment variables are loaded
load_dotenv(dotenv_path='/Users/shivacharan/retail_sales_analytics/.env') 


# Incoming file path
FILE_PATH = "/Users/shivacharan/retail_sales_analytics/data/incoming/sales_data_sample.csv"

def check_file():
    """
    Check if the incoming file exists.
    """
    if not os.path.exists(FILE_PATH):
        raise AirflowSkipException(f"File not found: {FILE_PATH}")

def run_notebook_with_papermill(**context):
    """
    Run a Jupyter notebook using Papermill.
    """
    raw_data = context['ti'].xcom_pull(task_ids='ingesting_to_raw_sales')
    print(f"this is the output: {raw_data}")
    os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@11'  # Adjust if needed
    os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:{os.environ['PATH']}"
    pm.execute_notebook(
        input_path='/Users/shivacharan/retail_sales_analytics/processing/etl_sales.ipynb',
        output_path='/Users/shivacharan/retail_sales_analytics/data/notebook_output/etl_sales_output.ipynb',
        parameters={'raw_data': raw_data},
    )


def run_script(script_path):
    """
    Run a Python script using subprocess.
    """
    subprocess.run(['python', script_path])
    

def call_snowflake_proc():
    """
    Call the Snowflake stored procedure to load data into dimension and fact tables.
    """
    # Load YAML config
    with open("/Users/shivacharan/retail_sales_analytics/config/sales_config.yaml") as f:
        config = yaml.safe_load(f)

    # Inject password from env
    config["snowflake"]["password"] = os.getenv("SNOWFLAKE_PASSWORD")
    db = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    
    # Check for missing password
    if not config["snowflake"]["password"]:
        raise ValueError("Missing SNOWFLAKE_PASSWORD environment variable")

    # Establish connection
    conn = snowflake.connector.connect(
        user=config["snowflake"]["user"],
        password=config["snowflake"]["password"],
        account=config["snowflake"]["account"],
        role=config["snowflake"]["role"],
        warehouse=config["snowflake"]["warehouse"]  
    )

    cursor = conn.cursor()
    try:
        cursor.execute(f"CALL {db}.{schema}.LOAD_DIM_TABLES_AND_FACT_TABLE()")
    finally:
        cursor.close()
        conn.close()

def send_email_custom(context):
    
    # SMTP configuration
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = 587
    smtp_user = os.getenv("AIRFLOW__SMTP__SMTP_USER")
    smtp_pass = os.getenv("AIRFLOW__SMTP__SMTP_PASSWORD")
    status = context.get("status", "Unknown")
    
    # Define email content
    msg = MIMEText(f"""
    <h3>Airflow DAG Notification</h3>
    <p><strong>DAG:</strong> {context['dag'].dag_id}</p>
    <p><strong>Run ID:</strong> {context['run_id']}</p>
    <p><strong>Status:</strong> {status.upper()}</p>
    <p><strong>Execution Time:</strong> {context['logical_date']}</p>
    <p>Check logs for more details.</p>
    """, "html")

    msg["Subject"] = f"[Airflow] DAG {context['dag'].dag_id} - {status.upper()}" 
    msg["From"] = smtp_user
    msg["To"] = "polishetty.shiva333@gmail.com"

    server = smtplib.SMTP(smtp_host, smtp_port)
    server.starttls()
    server.login(smtp_user, smtp_pass)
    server.send_message(msg)
    server.quit()



def success_wrapper(**context):
    context['status'] = 'success'
    send_email_custom(context)

def failure_wrapper(**context):
    context['status'] = 'failure'
    send_email_custom(context)


default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'catchup': False,
}

with DAG(
    dag_id='sales_etl_dag',
    schedule='0 8 * * *',  # 8:00 AM every day
    default_args=default_args,
    description='Run pipeline if file exists between 8-9 AM',
) as dag:

    # Task to check for the presence of the file
    check_file_task = PythonOperator(
        task_id='check_for_file',
        python_callable=check_file
    )

    # Task to ingest raw data (Using BashOperator to run a Python script)
    raw_ingestion = BashOperator(
        task_id="ingesting_to_raw_sales",
        bash_command="python '/Users/shivacharan/retail_sales_analytics/ingestion/batch/ingest_batch_sales.py'",
        do_xcom_push=True,
    )

    # Task to process the raw data using a Jupyter notebook
    process_raw_file = PythonOperator(
        task_id='run_etl_sales_notebook',
        python_callable=run_notebook_with_papermill,
    )

    # Task to load processed data to Snowflake stage
    load_to_stage = PythonOperator(
        task_id='load_to_snowflake_stage',
        python_callable=run_script,
        op_args=['/Users/shivacharan/retail_sales_analytics/processing/load_to_snowflake.py']
    )

    # Task to call the Snowflake stored procedure
    run_proc = PythonOperator(
        task_id='run_snowflake_proc',
        python_callable=call_snowflake_proc,
        retries=2,
        retry_delay=timedelta(minutes=2)
    )

    # Email notification tasks
    email_on_success = PythonOperator(
        task_id="email_on_success",
        python_callable=success_wrapper,
        op_kwargs={'context': '{{ task_instance.xcom_pull(task_ids="run_snowflake_proc") }}'},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )    

    email_on_failure = PythonOperator(
        task_id='email_on_failure',
        python_callable=failure_wrapper,
        op_kwargs={'context': '{{ task_instance.xcom_pull(task_ids="run_snowflake_proc") }}'},
        trigger_rule=TriggerRule.ONE_FAILED
    )

    
    check_file_task >> raw_ingestion >> process_raw_file >> load_to_stage >> run_proc >> [email_on_success, email_on_failure]

