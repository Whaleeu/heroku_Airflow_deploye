import pandas as pd
from data__api import gen_state
from processes import ParseFile, create_filestreams, load_file, upload_files
from data__api import gen_state
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import timedelta
from airflow.utils.email import send_email
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator



dag_email_recipient = ["whaleeu@gmail.com","muhammadyakub181@gmail.com"]
default_args = {
   'email': dag_email_recipient,
   'email_on_failure': True,}



dag = DAG(dag_id = "Weather_01",
        default_args=default_args,   
        schedule_interval = '@once',
        start_date  = datetime.datetime(2022, 9, 2),
        catchup = False,
        )


def process():
    """ 
    ---------------
    return None
    --------------
    
    Executes all functions in the process module. This extracts, tansform and upload weather
    data to s3 bucket in on call
    """

    state = pd.read_csv("/app/dags/list__of__capitals.csv").dropna()
    capitals = state['Capital'].values

    # Extractboto

    weather_data_gen = iter(gen_state(capitals))
    create_filestreams.has_been_called = False
    while True:
        
        try:
            jsondata = next(weather_data_gen)

            # Transform
            parser = ParseFile(json_handler=jsondata)
            data = {}

            city = parser.parse_area()
            weather, astronomy, hourly  = parser.parse_weather()

            

            data['city'] = city.dict()
            data['weather'] = weather.dict()
            data['astronomy'] = astronomy.dict()
            data['hourly'] = hourly

            # Load to s3

            if create_filestreams.has_been_called == False:
                create_filestreams(data=data)

            load_file(data=data)

        except StopIteration:
            print("Loading files completed!\nUploading files to aws s3 bucket...")
            upload_files()
            print('Uploaded')
            break


success_email_body = f"""
Hi, <br><br>
process_incoming_files DAG has been executed successfully at {datetime.datetime.now()}."""


# Execute
task_1_etl = PythonOperator(task_id = "Etl_task",
                                python_callable = process,
                                dag = dag,
                                )

send_mail = EmailOperator(
    task_id="send_mail",
    to=dag_email_recipient,
    subject='Airflow Success: process_incoming_files',
    html_content=success_email_body,
    dag=dag)

task_1_etl >> send_mail
