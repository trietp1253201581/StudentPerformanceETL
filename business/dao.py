import mysql.connector
from mysql.connector import Error
from model import StudentModel
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sql.sql_reader import SQLFileReader


class DAOException(Exception):
    def __init__(self, message):
        super().__init__(message)

class StudentPerformanceDAO:
    def __init__(self, host: str, db: str, user: str, password: str):
        self.connect_(host, db, user, password)
        self.get_sql_file_reader_()
        
    def connect_(self, host: str, db: str, user: str, password: str):
        try:
            self._connection = mysql.connector.connect(
                host=host,
                database=db,
                user=user,
                password=password
            )              
        except Error as ex:
            print("Error", ex)
            raise DAOException(ex.msg)
        
    def get_sql_file_reader_(self):
        self._sqlFileReader = SQLFileReader()
        try:
            curr_dir = os.path.dirname(__file__)
            sql_file_path = os.path.join(curr_dir, '..', 'sql', 'queries.sql')
            self._sqlFileReader.read(sql_file_path=sql_file_path)
            print(sql_file_path)
            print(self._sqlFileReader.get_enable_queries())
        except Exception as ioException:
            print(ioException)
    
    def insert(self, new_model: StudentModel):
        if not self._connection.is_connected():
            raise DAOException("Connection is null!")
        cursor = self._connection.cursor(prepared=True)
        insert_query = self._sqlFileReader.get_query_of('INSERT A NEW RECORD')
        values = (
            new_model.id,
            new_model.study_hours_per_week,
            new_model.attendance_rate,
            new_model.previous_grades,
            new_model.parcipate_on_act,
            new_model.parent_edu_level,
            new_model.passed
        )
        cursor.execute(insert_query, values)
        try:
            self._connection.commit()
        except Error as ex:
            raise DAOException(ex.msg)
    
    def close(self):
        if self._connection.is_connected():
            self._connection.close()
        
            