import numpy as np
import pandas as pd
from typing import Literal
from field import FieldName
class StudentModel:
    def __init__(self, id: str, 
                 study_hours_per_week: float, 
                 attendance_rate: float,
                 previous_grades: float, 
                 parcipate_on_act: str, 
                 parent_edu_level: str, 
                 passed: str):
        self.id = id
        self.study_hours_per_week = study_hours_per_week
        self.attendance_rate = attendance_rate
        self.previous_grades = previous_grades
        self.parcipate_on_act = parcipate_on_act
        self.parent_edu_level = parent_edu_level
        self.passed = passed
        
    @property
    def id(self):
        return self.__id
    
    @property
    def study_hours_per_week(self):
        return self.__study_hours_per_week
    
    @property
    def attendance_rate(self):
        return self.__attendance_rate
    
    @property
    def previous_grades(self):
        return self.__previous_grades
    
    @property
    def parcipate_on_act(self):
        return self.__parcipate_on_act
    
    @property
    def parent_edu_level(self):
        return self.__parent_edu_level
    
    @property
    def passed(self):
        return self.__passed
    
    @id.setter
    def id(self, id: str):
        if id is None:
            raise ValueError('Empty value for id')
        self.__id = id
    
    @study_hours_per_week.setter
    def study_hours_per_week(self, study_hours_per_week: float):
        if study_hours_per_week < 0:
            print('''Warning: Because study hours is non-negative
                  so the value to assign is None''')
            self.__study_hours_per_week = None
        elif np.isnan(study_hours_per_week):
            self.__study_hours_per_week = None
        else:
            self.__study_hours_per_week = study_hours_per_week
    
    @attendance_rate.setter
    def attendance_rate(self, attendance_rate: float):
        if attendance_rate < 0:
            print('''Warning: Because attendance rate is non-negative
                  so the value to assign is None''')
            self.__attendance_rate = None
        elif np.isnan(attendance_rate):
            self.__attendance_rate = None
        else:
            self.__attendance_rate = attendance_rate
    
    @previous_grades.setter
    def previous_grades(self, previous_grades: float):
        if previous_grades < 0 or previous_grades > 100:
            print('''Warning: Because attendance rate is in [0,100]
                  so the value to assign is the boundary''')
            if previous_grades < 0:
                self.__previous_grades = 0
            else:
                self.__previous_grades = 100
        elif np.isnan(previous_grades):
            self.__previous_grades = None
        else:
            self.__previous_grades = previous_grades
    
    @parcipate_on_act.setter
    def parcipate_on_act(self, parcipate_on_act: str):
        self.__parcipate_on_act = parcipate_on_act
    
    @parent_edu_level.setter
    def parent_edu_level(self, parent_edu_level: str):
        self.__parent_edu_level = parent_edu_level
    
    @passed.setter
    def passed(self, passed: str|Literal['Yes', 'No']):
        if passed not in ['Yes', 'No']:
            raise ValueError("Invalid value!")
        else:
            self.__passed = passed
            
    def __str__(self):
        s = f'Student(\n'
        s += f' id={self.id}\n'
        s += f' study hours per week={self.study_hours_per_week}\n'
        s += f' attendance_rate={self.attendance_rate}\n'
        s += f' previous grades={self.previous_grades}\n'
        s += f' participation in extra activities={self.parcipate_on_act}\n'
        s += f' parent education level={self.parent_edu_level}\n'
        s += f' passed={self.passed}\n'
        s += f')\n'
        return s
            


def create_model(record: pd.Series):
    return StudentModel(
        id=record[FieldName.STUDENT_ID],
        study_hours_per_week=record[FieldName.STUDY_HOURS],
        attendance_rate=record[FieldName.ATTENDANCE_RATE],
        previous_grades=record[FieldName.PREVIOUS_GRADES],
        parcipate_on_act=record[FieldName.PARTICIPATE_ON_ACT],
        parent_edu_level=record[FieldName.PARENT_EDU_LEVEL],
        passed=record[FieldName.PASSED]
    )

    
            
            
    
        
        
        
        
    
    