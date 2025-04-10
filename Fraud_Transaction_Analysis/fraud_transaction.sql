--QUERY 1: Select accounts with highest number of fraudolents transactions
SELECT nameOrig, 
COUNT(*) AS Num_frauds
FROM transactions
WHERE isFraud = 1
GROUP BY nameOrig
ORDER BY Num_frauds DESC
LIMIT 10;

--QUERY 2: Select largest fraud transfer by day
SELECT step,
FIRST_VALUE(nameOrig) OVER (PARTITION BY step ORDER BY amount DESC) AS Largest_Fraud_Account,
FIRST_VALUE(amount) OVER (PARTITION BY step ORDER BY amount DESC) AS Largest_Fraud_Amount
FROM transactions
WHERE (isFraud = 1) AND (type = 'TRANSFER')
GROUP BY step;

--QUERY 3: Select accounts with consecutive transactions flagged as fraudolents
SELECT *
FROM (
    SELECT nameOrig, step, isFlaggedFraud,
    LAG(isFlaggedFraud, 1, 0) OVER (PARTITION BY nameOrig ORDER BY step) AS prevFlaggedFraud
    FROM transactions
)
WHERE (isFlaggedFraud = 1) AND (prevFlaggedFraud = 1);

--QUERY 4: Select accounts with both endings and receiving transactions
SELECT DISTINCT nameOrig
FROM transactions 
WHERE (isFraud = 1) AND (type = 'TRANSFER')
INTERSECT
SELECT DISTINCT nameDest 
FROM transactions 
WHERE (isFraud = 1) AND (type = 'TRANSFER');

--QUERY 5: Account with frauds detection 
WITH flagged_transactions AS
(
    SELECT nameOrig, step, amount
    FROM transactions
    WHERE (isFlaggedFraud = 1) AND (type = 'TRANSFER')
),
anomalous_amount AS
(
    SELECT nameOrig, step, amount
    FROM transactions
    WHERE (amount > (SELECT AVG(amount) + 2 * STDEV(amount) FROM transactions)) AND (type = 'TRANSFER')
),
circular_transactions AS 
(
    SELECT nameOrig, step, amount
    FROM transactions
    WHERE (nameOrig = nameDest) AND (type = 'TRANSFER')
)
SELECT ft.nameOrig
FROM flagged_transactions AS ft
JOIN anomalous_amount AS aa
ON ((ft.nameOrig = aa.nameOrig) AND (ft.step = aa.step))
JOIN circular_transactions AS ct
ON ((ft.nameOrig = ct.nameOrig) AND (ft.step = ct.step));

--QUERY 6: Create trigger to detect suspicious transactions
CREATE TRIGGER fraud_detection 
BEFORE INSERT ON transactions
BEGIN
    SELECT
        CASE
            WHEN ((NEW.isFraud = 1) OR (NEW.amount > NEW.oldbalanceOrg)
            OR (NEW.nameOrig = NEW.nameDest)) THEN
            RAISE (ABORT, 'Suspicious transaction')
        END
END;
