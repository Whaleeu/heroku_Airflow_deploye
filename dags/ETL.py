import pandas as pd
from data__api import gen_state
from processes import ParseFile, create_filestreams, load_file, upload_files
from data__api import gen_state
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


dag = DAG(dag_id = "Weather_01",
        schedule_interval = '@once',
        start_date  = datetime.datetime(2022, 8, 31),
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


def display_logs():
    print("All Executed")


# Execute
task_1_etl = PythonOperator(task_id = "Etl_task",
                                python_callable = process,
                                dag = dag,
                                )

task_2_notification = PythonOperator(task_id = "Notify",
                        python_callable = display_logs,
                        dag = dag,
                        )

task_1_etl >> task_2_notification
