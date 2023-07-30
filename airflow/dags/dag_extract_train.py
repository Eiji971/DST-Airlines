from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime, timedelta 
from airflow.models import Variable 
from fct_script.flight_extraction import get_schedule
from fct_script.Authentication_key_retrieval import get_valid_token
from fct_script.data_preprocessor_fct import preprocess_data_air
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import confusion_matrix, classification_report
import json 
import requests
import os 
import pandas as pd 
import numpy as np
from joblib import dump

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

client_id = 'pppgbsjxaegfhhh5ehjjgstnb'
client_secret = '6aHXhkBTH6'


def get_flight_status():

    bearer_token = get_valid_token()

    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Accept": "application/json"
    }

    apiUrl2 = "https://api.lufthansa.com/v1/flight-schedules/flightschedules/passenger?airlines=LH&startDate=21JUN23&endDate=22JUL23&daysOfOperation=1234567&timeMode=UTC"
    directory = "./rawdata"
    os.makedirs(directory, exist_ok=True)   
    
    filename = "./rawdata/flight_schedule.json"
    
    response = requests.get(apiUrl2, headers=headers)
    
    dataJson = response.json()
    with open(filename, 'w') as file:
        json.dump(dataJson, file)

    directory = "./dataclean"
    os.makedirs(directory, exist_ok=True)  

    get_schedule(filename)


def train_model():
    client = MongoClient('mongodb://127.0.0.1:27017/airlines_data')
    db = client.airlines_data
    collection = db.flight_data

    # Retrieve the documents from the MongoDB collection and convert to a list of dictionaries
    data = list(collection.find({}))

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(data)
    
    X_train_preprocessed, y_train_encoded, X_test_preprocessed, y_test_encoded = preprocess_data_air(df)

    rf = RandomForestClassifier(random_state=42, n_estimators=300, min_samples_split=2, max_depth=10)
    gb = GradientBoostingClassifier(random_state=42, learning_rate=0.05, max_depth=5, max_features='sqrt', min_samples_leaf=1, min_samples_split=2, n_estimators=100, subsample=0.8)

    # Fit the base models on the training data
    rf.fit(X_train_preprocessed, y_train_encoded)
    gb.fit(X_train_preprocessed, y_train_encoded)

    # Make predictions on the test data using the base models
    y_rf_pred = rf.predict(X_test_preprocessed)
    y_gb_pred = gb.predict(X_test_preprocessed)

    # Perform model blending
    y_blend_pred = np.round((y_rf_pred + y_gb_pred) / 2)

    # Print classification report
    print(classification_report(y_test_encoded, y_blend_pred))

    # Calculate the confusion matrix
    cm = confusion_matrix(y_test_encoded, y_blend_pred)

    # Convert the confusion matrix array into a DataFrame
    cm_df = pd.DataFrame(cm, index=['Class 0', 'Class 1', 'Class 2', 'Class 3'], columns=['Predicted Class 0', 'Predicted Class 1', 'Predicted Class 2', 'Predicted Class 3'])
    print(confusion_matrix(y_test_encoded, y_test_encoded))
    # Display the confusion matrix DataFrame
    print(cm_df)

    dump(gb, './models/gradient_boost_model.pkl')
    dump(rf, './models/random_forest_model.pkl')

dag = DAG(
    'retrival_training_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

fetch_and_save_task = PythonOperator(
    task_id='fetch_and_save_flight',
    python_callable=get_flight_status,
    dag=dag,
)

train_and_save_task = PythonOperator(
    task_id='train_and_save_model',
    python_callable=train_model,
    dag=dag,
)


fetch_and_save_task >> train_and_save_task