import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

import time
from typing import Literal
import pandas as pd
import numpy as np
from kaggle.api.kaggle_api_extended import KaggleApi

from model import create_model
from field import FieldName
from dao import StudentPerformanceDAO, DAOException, NotExistDataException

import logging
# Thiết lập config cho log
logging.basicConfig(filename='business/etl_log.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


dataset_id = 'souradippal/student-performance-prediction'
data_dir = 'business/data'
file_dir = data_dir + '/student-performance-prediction/student_performance_prediction.csv'

def extract() -> pd.DataFrame:
    """
    Tải và đọc dữ liệu từ Kaggle dataset.

    Returns:
        pd.DataFrame: Dữ liệu thô từ file CSV.

    Raises:
        Exception: Nếu có lỗi xảy ra trong quá trình tải dữ liệu.
    """
    logging.info('Extracting data...')
    os.environ['KAGGLE_CONFIG_DIR'] = '~/.kaggle'
    # Xác thực và tải dataset từ Kaggle 
    kaggle_api = KaggleApi()
    try:
        kaggle_api.authenticate()
        logging.info('Successfully authenticate Kaggle API.')
    except Exception as e:
        logging.error(f"Failed to authenticate Kaggle API.")
        raise e
    
    start_time = time.time()
    
    #Tải
    try:
        kaggle_api.dataset_download_files(dataset_id, data_dir, force=True, unzip=True)
        logging.info(f"Dataset downloaded successfully from {dataset_id}.")
    except Exception as e:
        logging.error(f'Failed to download datasets at {dataset_id}.')
        raise e
    raw_df = pd.read_csv(file_dir)
    
    end_time = time.time()
    msg = f'Successfully extract: {len(raw_df)} records. Elapsed Time: {end_time-start_time:.4f}s'
    print(msg)
    logging.info(msg)
    return raw_df

def transform(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Chuyển đổi dữ liệu thô thành dữ liệu đã được xử lý.

    Args:
        raw_df (pd.DataFrame): Dữ liệu thô.

    Returns:
        pd.DataFrame: Dữ liệu đã được xử lý.

    Raises:
        Exception: Nếu có lỗi xảy ra trong quá trình chuyển đổi dữ liệu.
    """
    logging.info('Transforming...')
    start_time = time.time()
    df = raw_df

    # Xóa các giá trị NaN trong cột mục tiêu
    df.dropna(subset=[FieldName.PASSED], inplace=True)
    df.reset_index(inplace=True, drop=True)

    # Đánh dấu giá trị bị thiếu hoặc dữ liệu sai bằng np.nan
    # Giờ học phải là số không âm
    df[FieldName.STUDY_HOURS] = df[FieldName.STUDY_HOURS].apply(lambda x: x if x >= 0 else np.nan)

    # Tỷ lệ tham gia phải là số không âm
    df[FieldName.ATTENDANCE_RATE] = df[FieldName.ATTENDANCE_RATE].apply(lambda x: x if (x>=0 and x<=100) else np.nan)

    # Điểm số trước đó phải trong khoảng [0,100]
    df[FieldName.PREVIOUS_GRADES] = df[FieldName.PREVIOUS_GRADES].clip(0, 100)
    df.fillna({FieldName.PREVIOUS_GRADES: np.nan}, inplace=True)

    # Điền giá trị NaN trong các đặc trưng phân loại bằng "Unknown"
    df.fillna({FieldName.PARTICIPATE_ON_ACT: 'Unknown',
               FieldName.PARENT_EDU_LEVEL: 'Unknown'}, inplace=True)

    end_time = time.time()
    msg = f'Successfully transform: {len(df)} records. Elapsed Time: {end_time-start_time:.4f}s'
    print(msg)
    logging.info(msg)
    
    return df  

def load(modified_df: pd.DataFrame):
    """
    Tải dữ liệu đã được xử lý vào cơ sở dữ liệu.

    Args:
        modified_df (pd.DataFrame): Dữ liệu đã được xử lý.

    Raises:
        DAOException: Nếu có lỗi xảy ra trong quá trình tải dữ liệu vào cơ sở dữ liệu.
    """
    logging.info('Loading data...')
    models = modified_df.apply(create_model, axis=1).values
    spDAO = StudentPerformanceDAO('localhost', 'student_performance_etl', 'root', 'Asensio1234@')
    start_time = time.time()
    update_records = [0, 0]
    insert_records = [0, 0]

    # Xử lý từng mô hình trong dữ liệu
    for model in models:
        try:
            spDAO.get(model.id)
            update_records[0] += 1
            spDAO.update(model)
            update_records[1] += 1
        except NotExistDataException as ne:
            insert_records[0] += 1
            spDAO.insert(model)
            insert_records[1] += 1
        except DAOException as e:
            print("Failed at ", model.id)
            logging.error(f'Failed at {model.id}')

    end_time = time.time()
    msg = f'Load info: \n'
    msg += f'\tUpdate Successfully: {update_records[1]}/{update_records[0]}\n'
    msg += f'\tInsert Successfully: {insert_records[1]}/{insert_records[0]}\n'
    msg += f'Elapsed Time: {end_time-start_time:.4f}s'
    print(msg)
    logging.info(msg)
    spDAO.close()
    
def reset():
    """
    Xóa tất cả các bản ghi trong cơ sở dữ liệu.

    Raises:
        DAOException: Nếu có lỗi xảy ra trong quá trình xóa dữ liệu.
    """
    spDAO = StudentPerformanceDAO('localhost', 'student_performance_etl', 'root', 'Asensio1234@')
    try:
        spDAO.delete_all()
        print('Successfully reset!')
        logging.info('Reset data...')
    except DAOException as e:
        print(e)
        logging.error('Error when reset!')
    
            
def etl_process(load_for: int|None = None, 
                 strategy: Literal['head', 'tail', 'random']|None = 'head',
                 need_reset: bool = False):
    """
    Thực hiện quá trình ETL (Extract, Transform, Load) hoàn chỉnh.

    Args:
        load_for (int|None): Số lượng bản ghi cần tải vào cơ sở dữ liệu, mặc định là None
        strategy (Literal['head', 'tail', 'random']|None): Chính sách để lấy các bản ghi, mặc định là 'head'
        need_reset (bool): Yêu cầu reset lại các bản ghi trong CSDL (tức là xóa tất cả), mặc định là False
    Raises:
        Exception: Nếu có lỗi xảy ra trong quá trình ETL.
    """
    # Extract
    raw_df = extract()
    
    # Transform
    modified_df = transform(raw_df)
    
    # Load
    # Check reset 
    if need_reset:
        reset()
    # Check strategy
    if isinstance(load_for, int):
        if strategy == 'head':
            modified_df = modified_df.head(load_for)
        elif strategy == 'tail':
            modified_df = modified_df.tail(load_for)
        elif strategy == 'random':
            modified_df = modified_df.sample(load_for, random_state=42)
        
    load(modified_df)