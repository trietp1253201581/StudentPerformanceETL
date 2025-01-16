import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import mysql.connector
from mysql.connector import Error

from model import StudentModel, create_model
from sql.sql_reader import SQLFileReader


class DAOException(Exception):
    """
    Ngoại lệ chung cho lớp DAO (Data Access Object).

    Attributes:
        message (str): Thông điệp của ngoại lệ.
    """
    
    def __init__(self, message):
        """
        Hàm khởi tạo của DAOException.

        Args:
            message (str): Thông điệp của ngoại lệ.
        """
        super().__init__(message)


class NotExistDataException(DAOException):
    """
    Ngoại lệ được ném ra khi dữ liệu không tồn tại.

    Attributes:
        message (str): Thông điệp của ngoại lệ.
    """

    def __init__(self, message):
        """
        Hàm khởi tạo của NotExistDataException.

        Args:
            message (str): Thông điệp của ngoại lệ.
        """
        super().__init__(message)


class StudentPerformanceDAO:
    """
    Lớp để thao tác với cơ sở dữ liệu liên quan đến hiệu suất học tập của sinh viên.

    Attributes:
        _connection: Kết nối cơ sở dữ liệu.
        _sqlFileReader: Đối tượng đọc file SQL.
    """

    def __init__(self, host: str, db: str, user: str, password: str):
        """
        Hàm khởi tạo của StudentPerformanceDAO.

        Args:
            host (str): Địa chỉ host của cơ sở dữ liệu.
            db (str): Tên cơ sở dữ liệu.
            user (str): Tên người dùng của cơ sở dữ liệu.
            password (str): Mật khẩu của cơ sở dữ liệu.
        """
        self.connect_(host, db, user, password)
        self.get_sql_file_reader_()
        
    def connect_(self, host: str, db: str, user: str, password: str):
        """
        Kết nối tới cơ sở dữ liệu.

        Args:
            host (str): Địa chỉ host của cơ sở dữ liệu.
            db (str): Tên cơ sở dữ liệu.
            user (str): Tên người dùng của cơ sở dữ liệu.
            password (str): Mật khẩu của cơ sở dữ liệu.

        Raises:
            DAOException: Nếu kết nối tới cơ sở dữ liệu thất bại.
        """
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
        """
        Lấy đối tượng đọc file SQL.

        Raises:
            Exception: Nếu đọc file SQL thất bại.
        """
        self._sqlFileReader = SQLFileReader()
        try:
            curr_dir = os.path.dirname(__file__)
            sql_file_path = os.path.join(curr_dir, '..', 'sql', 'queries.sql')
            self._sqlFileReader.read(sql_file_path=sql_file_path)
        except Exception as ioException:
            print(ioException)
    
    def insert(self, new_model: StudentModel) -> None:
        """
        Thêm một bản ghi mới vào cơ sở dữ liệu.

        Args:
            new_model (StudentModel): Mô hình sinh viên cần thêm.

        Raises:
            DAOException: Nếu kết nối tới cơ sở dữ liệu không tồn tại hoặc lỗi khi thêm bản ghi.
        """
        if not self._connection.is_connected():
            raise DAOException("Connection is null!")
        
        # Chuẩn bị câu lệnh truy vấn và giá trị cần thêm
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
        cursor.close()
        
        # Commit các thay đổi vào cơ sở dữ liệu
        try:
            self._connection.commit()
        except Error as ex:
            raise DAOException(ex.msg)
        
    def get_all(self) -> list[StudentModel]:
        """
        Lấy tất cả các bản ghi từ cơ sở dữ liệu.

        Returns:
            list[StudentModel]: Danh sách các mô hình sinh viên.

        Raises:
            DAOException: Nếu kết nối tới cơ sở dữ liệu không tồn tại hoặc lỗi khi lấy bản ghi.
            NotExistDataException: Nếu không tìm thấy dữ liệu.
        """
        if not self._connection.is_connected():
            raise DAOException("Connection is null!")
        
        # Thực thi câu lệnh truy vấn để lấy tất cả các bản ghi
        cursor = self._connection.cursor(prepared=False)
        get_query = self._sqlFileReader.get_query_of('GET ALL')
        cursor.execute(get_query)
        result = cursor.fetchall()
        cursor.close()
        
        if result is None:
            raise NotExistDataException("Not found!")
        
        # Chuyển đổi kết quả thành danh sách các mô hình sinh viên
        try:
            self._connection.commit()
            models = []
            for row in result:
                models.append(create_model(row))
            return models
        except Error as ex:
            raise DAOException(ex.msg)
        
    def get(self, id: str) -> StudentModel:
        """
        Lấy một bản ghi từ cơ sở dữ liệu theo mã sinh viên.

        Args:
            id (str): Mã sinh viên.

        Returns:
            StudentModel: Mô hình sinh viên.

        Raises:
            DAOException: Nếu kết nối tới cơ sở dữ liệu không tồn tại hoặc lỗi khi lấy bản ghi.
            NotExistDataException: Nếu không tìm thấy dữ liệu.
        """
        if not self._connection.is_connected():
            raise DAOException("Connection is null!")
        
        # Thực thi câu lệnh truy vấn để lấy bản ghi theo mã sinh viên
        cursor = self._connection.cursor(prepared=True)
        get_query = self._sqlFileReader.get_query_of('GET A RECORD BY ID')
        values = (id,)
        cursor.execute(get_query, values)
        result = cursor.fetchone()
        cursor.close()
        
        if result is None:
            raise NotExistDataException("Not found!")
        
        # Chuyển đổi kết quả thành mô hình sinh viên
        try:
            self._connection.commit()
            return create_model(result)
        except Error as ex:
            raise DAOException(ex.msg)
    
    def update(self, model: StudentModel):
        """
        Cập nhật một bản ghi trong cơ sở dữ liệu.

        Args:
            model (StudentModel): Mô hình sinh viên cần cập nhật.

        Raises:
            DAOException: Nếu kết nối tới cơ sở dữ liệu không tồn tại hoặc lỗi khi cập nhật bản ghi.
        """
        if not self._connection.is_connected():
            raise DAOException("Connection is null!")
        
        # Chuẩn bị câu lệnh truy vấn và giá trị cần cập nhật
        cursor = self._connection.cursor(prepared=True)
        update_query = self._sqlFileReader.get_query_of('UPDATE A RECORD')
        values = (
            model.study_hours_per_week,
            model.attendance_rate,
            model.previous_grades,
            model.parcipate_on_act,
            model.parent_edu_level,
            model.passed,
            model.id
        )
        cursor.execute(update_query, values)
        cursor.close()
        
        # Commit các thay đổi vào cơ sở dữ liệu
        try:
            self._connection.commit()
        except Error as ex:
            raise DAOException(ex.msg)
        
    def delete_all(self):
        """
        Xóa tất cả các bản ghi trong cơ sở dữ liệu.

        Raises:
            DAOException: Nếu kết nối tới cơ sở dữ liệu không tồn tại hoặc lỗi khi xóa bản ghi.
        """
        if not self._connection.is_connected():
            raise DAOException("Connection is null!")
        
        # Thực thi câu lệnh truy vấn để xóa tất cả các bản ghi
        cursor = self._connection.cursor(prepared=False)
        delete_query = self._sqlFileReader.get_query_of('DELETE ALL')
        cursor.execute(delete_query)
        cursor.close()
    
        # Commit các thay đổi vào cơ sở dữ liệu
        try:
            self._connection.commit()
        except Error as ex:
            raise DAOException(ex.msg)
    
    def close(self):
        """
        Đóng kết nối tới cơ sở dữ liệu.
        """
        if self._connection.is_connected():
            self._connection.close()