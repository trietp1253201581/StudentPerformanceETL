--INSERT A NEW RECORD
INSERT INTO student_performance(student_id, study_hours_per_week,
attendance_rate, previous_grades, participate_in_act,
parent_edu_level, passed) 
VALUES(%s, %s, %s, %s, %s, %s, %s);

--GET ALL
SELECT * FROM student_performance
ORDER BY student_id, passed;

--GET A RECORD BY ID
SELECT * FROM student_performance
WHERE student_id = %s;

--UPDATE A RECORD
UPDATE student_performance SET study_hours_per_week = %s, 
attendance_rate = %s, previous_grades = %s, participate_in_act = %s,
parent_edu_level = %s, passed = %s
WHERE student_id = %s;

--DELETE ALL
DELETE FROM student_performance;
