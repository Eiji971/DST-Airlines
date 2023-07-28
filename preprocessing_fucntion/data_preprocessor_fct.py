import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
from sklearn.preprocessing import StandardScaler, LabelEncoder, OrdinalEncoder, OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
import numpy as np

def preprocess_data(df):
    # Initialize the Status column with missing values
    df['ArrStatus'] = pd.NA

    # Compare the columns and update the Status column
    df.loc[df['ArrSchedTime'] > df['ArrActualTime'], 'ArrStatus'] = 'Flight Arrive Late'
    df.loc[df['ArrSchedTime'] < df['ArrActualTime'], 'ArrStatus'] = 'Flight Arrive Early'
    df.loc[df['ArrSchedTime'] == df['ArrActualTime'], 'ArrStatus'] = 'Flight Arrive On Time'

    # Initialize the Status column with missing values
    df['DepartStatus'] = pd.NA

    # Compare the columns and update the Status column
    df.loc[df['DepSchedTime'] > df['DepActualTime'], 'DepartStatus'] = 'Flight Depart Late'
    df.loc[df['DepSchedTime'] < df['DepActualTime'], 'DepartStatus'] = 'Flight Depart Early'
    df.loc[df['DepSchedTime'] == df['DepActualTime'], 'DepartStatus'] = 'Flight Depart On Time'

    # Drop unnecessary columns
    df_clean = df.drop(['DepSchedDate', 'DepActualDate', 'ArrSchedDate', 'ArrActualDate', 'DepStatusDesc',
                        'DepStatusCode', 'MarkFlightNumber', 'OpFlightNumber', 'ArrStatusDesc',
                        'ArrStatusCode', 'MarkAirlineID', 'DepActualTime', 'ArrActualTime'], axis=1)

    # Split the Scheduled hours of the flight into an hour and a minute column
    df_clean[['Dephours', 'Depminutes']] = df_clean['DepSchedTime'].str.split(':', expand=True).astype(int)
    df_clean[['Arrhours', 'Arrminutes']] = df_clean['ArrSchedTime'].str.split(':', expand=True).astype(int)
    df_clean.drop(['DepSchedTime', 'ArrSchedTime'], axis=1, inplace=True)

    # Type attribution
    df_clean['DepartStatus'] = df_clean['DepartStatus'].astype(str)
    df_clean['ArrStatus'] = df_clean['ArrStatus'].astype(str)
    df_clean['AircraftCode'] = df_clean['AircraftCode'].astype(str)
    df_clean['OpAirlineID'] = df_clean['OpAirlineID'].astype(str)
    df_clean['ArrivalAirportCode'] = df_clean['ArrivalAirportCode'].astype(str)
    df_clean['DepAirportCode'] = df_clean['DepAirportCode'].astype(str)

    # Separate features and target
    # Define features and target
    feats = df_clean.drop("ArrStatus", axis=1)
    target = df_clean["ArrStatus"]

    # Define numeric and categorical features
    numeric_features = ["Dephours", "Depminutes", "Arrhours", "Arrminutes"]
    categorical_features = ["DepAirportCode", "ArrivalAirportCode", "OpAirlineID", "AircraftCode", "DepartStatus"]

    # Create the pipeline for numeric features
    numeric_pipeline = Pipeline([
        ('scaler', StandardScaler())
    ])

    # Create the pipeline for categorical features
    categorical_pipeline = Pipeline([
        ('imputer', SimpleImputer(strategy='most_frequent')),
        ('encoder', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=np.nan))
    ])

    # Create the column transformer to apply different pipelines to different feature subsets
    preprocessor = ColumnTransformer([
        ('numeric', numeric_pipeline, numeric_features),
        ('categorical', categorical_pipeline, categorical_features)
    ])

    # Apply the preprocessing pipeline to the data
    feats_preprocessed = preprocessor.fit_transform(feats)

    # Convert the target labels using label encoder
    label_encoder = LabelEncoder()
    target_encoded = label_encoder.fit_transform(target)

    return feats_preprocessed, target_encoded


def preprocess_data_air(df):
    df['ArrStatus'] = pd.NA  # Initialize the Status column with missing values

    # Compare the columns and update the Status column
    df.loc[df['ArrSchedTime'] > df['ArrActualTime'], 'ArrStatus'] = 'Flight Arrive Late'
    df.loc[df['ArrSchedTime'] < df['ArrActualTime'], 'ArrStatus'] = 'Flight Arrive Early'
    df.loc[df['ArrSchedTime'] == df['ArrActualTime'], 'ArrStatus'] = 'Flight Arrive On Time'

    df['DepartStatus'] = pd.NA  # Initialize the Status column with missing values

    # Compare the columns and update the Status column
    df.loc[df['DepSchedTime'] > df['DepActualTime'], 'DepartStatus'] = 'Flight Depart Late'
    df.loc[df['DepSchedTime'] < df['DepActualTime'], 'DepartStatus'] = 'Flight Depart Early'
    df.loc[df['DepSchedTime'] == df['DepActualTime'], 'DepartStatus'] = 'Flight Depart On Time'

    # Drop unnecessary columns 
    df_clean = df.drop(['DepSchedDate', 'DepActualDate', 'ArrSchedDate', 'ArrActualDate', 'DepStatusDesc', 
                        'DepStatusCode', 'MarkFlightNumber', 'OpFlightNumber', 'ArrStatusDesc',
                        'ArrStatusCode', 'MarkAirlineID', 'DepActualTime', 'ArrActualTime'], axis=1)

    # Split the Scheduled hours of the flight in an hour and a minute column
    df_clean[['Dephours', 'Depminutes']] = df_clean['DepSchedTime'].str.split(':', expand=True).astype(int)
    df_clean[['Arrhours', 'Arrminutes']] = df_clean['ArrSchedTime'].str.split(':', expand=True).astype(int)
    df_clean.drop(['DepSchedTime', 'ArrSchedTime'], axis=1, inplace=True)

    # Type attribution 
    df_clean['DepartStatus'] = df_clean['DepartStatus'].astype(str)
    df_clean['ArrStatus'] = df_clean['ArrStatus'].astype(str)
    df_clean['AircraftCode'] = df_clean['AircraftCode'].astype(str)
    df_clean['OpAirlineID'] = df_clean['OpAirlineID'].astype(str)
    df_clean['ArrivalAirportCode'] = df_clean['ArrivalAirportCode'].astype(str)
    df_clean['DepAirportCode'] = df_clean['DepAirportCode'].astype(str)


    # Separate features and target
    feats = df_clean.drop("ArrStatus", axis=1)
    target = df_clean["ArrStatus"]

    # Split the data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(feats, target, test_size=0.2, random_state=42)

    # Define numeric and categorical features
    numeric_features = ["Dephours", "Depminutes", "Arrhours", "Arrminutes"]
    categorical_features = ["DepAirportCode", "ArrivalAirportCode", "OpAirlineID", "AircraftCode", "DepartStatus"]

    # Create the pipeline for numeric features
    numeric_pipeline = Pipeline([
        ('scaler', StandardScaler())
    ])

    # Create the pipeline for categorical features
    categorical_pipeline = Pipeline([
        ('imputer', SimpleImputer(strategy='most_frequent')),
        ('encoder', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=np.nan))
    ])

    # Create the column transformer to apply different pipelines to different feature subsets
    preprocessor = ColumnTransformer([
        ('numeric', numeric_pipeline, numeric_features),
        ('categorical', categorical_pipeline, categorical_features)
    ])

    # Apply the preprocessing pipeline to the training data
    X_train_preprocessed = preprocessor.fit_transform(X_train)

    # Apply the preprocessing pipeline to the test data
    X_test_preprocessed = preprocessor.transform(X_test)

    # Convert the target labels using label encoder
    label_encoder = LabelEncoder()
    y_train_encoded = label_encoder.fit_transform(y_train)
    y_test_encoded = label_encoder.transform(y_test)

    return X_train_preprocessed, y_train_encoded, X_test_preprocessed, y_test_encoded