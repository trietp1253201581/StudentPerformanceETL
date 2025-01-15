import os
import pandas as pd
import numpy as np
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from model import create_model
from opendatasets import download_kaggle_dataset
from field import FieldName
from dao import StudentPerformanceDAO, DAOException

dataset_url = 'https://www.kaggle.com/datasets/souradippal/student-performance-prediction'
data_dir = 'business/data'
file_dir = data_dir + '/student-performance-prediction/student_performance_prediction.csv'

def extract() -> pd.DataFrame:
    os.environ['KAGGLE_CONFIG_DIR'] = 'business'
    download_kaggle_dataset(
        dataset_url=dataset_url,
        data_dir=data_dir,
    )
    raw_df = pd.read_csv(file_dir)
    return raw_df

def transform(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df
    # Remove NaN in target column
    df.dropna(subset=[FieldName.PASSED], inplace=True)
    df.reset_index(inplace=True, drop=True)
    # We will mark missing value or wrong data by np.nan
    # Study Hours must be non-negative
    df[FieldName.STUDY_HOURS] = df[FieldName.STUDY_HOURS].apply(lambda x: x if x >= 0 else np.nan)
    # Attendance rate must be non-negative
    df[FieldName.ATTENDANCE_RATE] = df[FieldName.ATTENDANCE_RATE].apply(lambda x: x if (x>=0 and x<=100) else np.nan)
    # Previous grade must be in [0,100]
    df[FieldName.PREVIOUS_GRADES] = df[FieldName.PREVIOUS_GRADES].clip(0, 100)
    df.fillna({FieldName.PREVIOUS_GRADES: np.nan}, inplace=True)
    # Fill NaN in categorical features is "Unknown"
    df.fillna({FieldName.PARTICIPATE_ON_ACT: 'Unknown',
               FieldName.PARENT_EDU_LEVEL: 'Unknown'}, inplace=True)
    return df  

def load(modified_df: pd.DataFrame):
    models = modified_df.apply(create_model, axis=1).values
    spDAO = StudentPerformanceDAO('localhost', 'student_performance_etl', 'root', 'Asensio1234@')
    for model in models:
        try:
            spDAO.insert(model)
        except DAOException as e:
            print(e)
            
def full_process():
    raw_df = extract()
    modified_df = transform(raw_df)
    load(modified_df)