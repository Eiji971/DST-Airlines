from fastapi import FastAPI, Request, Depends, HTTPException, UploadFile, File
from fastapi.security import HTTPBasic, HTTPBasicCredentials, OAuth2PasswordBearer
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Union
import random
from io import StringIO
import pandas as pd 
import numpy as np
import json
import base64
import csv
from joblib import load
from preprocessing_fucntion.data_preprocessor_fct import preprocess_data
from sklearn.metrics import confusion_matrix

gb_model = load('./models/gradient_boost_model.pkl')
rf_model = load('./models/random_forest_model.pkl')


api = FastAPI()

# Authentification for the API

# Defining username and password
USERNAME = "Emgri"
PASSWORD = "BootcampDE0523"

# Using OAuth2PasswordBearer to generate the authentification with token
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Function to check the username and password
def verify_user(username: str, password: str):
    if username == USERNAME and password == PASSWORD:
        return True
    return False

# Function to get the current username using Depends to validate the authentification
def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials = token.split(":")
    if len(credentials) != 2:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    username, password = credentials[0], credentials[1]
    if not verify_user(username, password):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    return username

# Secured path requiring an authentification
@api.get("/secure-data/")
def get_secure_data(current_user: str = Depends(get_current_user)):
    return {"message": f"Welcome, {current_user}. You are authorized to access this secure data."}

@api.get("/")
def root():
    return {"message": "Welcome to Your Flight Status Classification FastAPI"}


@api.post("/predict")
async def predict_delay(file: UploadFile = File(...)):
    try:
        content = await file.read()
        content = content.decode("utf-8")
        df = pd.read_csv(StringIO(content))
        # Perform preprocessing on the DataFrame
        try:
            feats_preprocessed, target_encoded = preprocess_data(df)
            
        except Exception as e:
            raise Exception("Error in preprocessing data: " + str(e))
        # Get predictions from both models
        try:
            
            gb_prediction = gb_model.predict(feats_preprocessed)
            rf_prediction = rf_model.predict(feats_preprocessed)
            
        except Exception as e:
            raise HTTPException(status_code=500, detail="Error in model prediction: " + str(e))
        
        # Perform blending of the predictions
        blended_prediction = (gb_prediction + rf_prediction) / 2

        return {"predictions": blended_prediction.tolist()}

    except Exception as e:
        raise HTTPException(status_code=500, detail="Error in processing file: " + str(e))





