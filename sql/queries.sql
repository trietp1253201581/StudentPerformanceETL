--SELECT
SELECT * FROM users
WHERE users.asa = ?;

--INSERT A NEW RECORD
INSERT INTO users(asa, bsb) 
VALUES(?, ?, ?);