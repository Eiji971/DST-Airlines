from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 27, 21, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

today = datetime.now().date()
formattedDate = today.strftime("%Y-%m-%d")

dag = DAG(
    'flight_status_to_mongodb',
    default_args=default_args,
    schedule_interval = '* * */1 * *',
    catchup=False
)

# Task to copy the CSV file to the MongoDB container
copy_csv_task = BashOperator(
    task_id='copy_csv_to_docker',
    bash_command=f'docker cp /Users/emerybosc/DST-Airlines/data/extractedcsv/flight_status{formattedDate}.csv mongodb:/flight_status{formattedDate}.csv',
    dag=dag,
)

# Task to import the CSV file into MongoDB using mongoimport
import_csv_task = BashOperator(
    task_id='import_to_mongo',
    bash_command=f'docker exec mongodb mongoimport --host 127.0.0.1 --port 27017 --db mongodb --collection flightschedule --type csv --headerline --file /flight_status{formattedDate}.csv',
    dag=dag,
)
# Task to delete the CSV file from the container
delete_csv_task = BashOperator(
    task_id='delete_csv_from_docker',
    bash_command=f'docker exec mongodb rm /flight_status{formattedDate}.csv',
    dag=dag
)

# Define the task dependencies
copy_csv_task >> import_csv_task >> delete_csv_task