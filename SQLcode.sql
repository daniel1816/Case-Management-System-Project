DROP DATABASE IF EXISTS CMS;
CREATE DATABASE CMS;
GO
USE CMS;
GO

--create tables with PK and FK constraints
CREATE TABLE associate(
	Emp_ID INT NOT NULL,
	First_name VARCHAR(25) NOT NULL,
	Last_name VARCHAR(25) NOT NULL,
	Start_date DATE NULL,
	Rate SMALLMONEY NULL
	PRIMARY KEY(Emp_ID)
);
go

CREATE TABLE client (
	Client_ID INT NOT NULL,
	[Name] VARCHAR(50) NOT NULL,
	[Address] VARCHAR(100)  NULL,
	Billing_atty INT  NULL,
	PRIMARY KEY(Client_ID),
	FOREIGN KEY(Billing_atty) REFERENCES associate(Emp_ID)
);

CREATE TABLE patentCase (
	Case_ID INT NOT NULL,
	Status VARCHAR(10) NOT NULL,
	Filing_date DATE NULL,
	Issue_date DATE NULL,
	WA_ID INT NULL,
	Client_ID INT NULL
	PRIMARY KEY(Case_ID),
	FOREIGN KEY(WA_ID) REFERENCES associate(Emp_ID),
	FOREIGN KEY(Client_ID) REFERENCES client(Client_ID)
);


GO
CREATE TABLE correspondence(
	Correspondence_ID INT NOT NULL,
	Case_ID INT NOT NULL,
	Correspondence_type VARCHAR(50) NOT NULL,
	MLD DATE NOT NULL,
	DueDate DATE NULL,
	PRIMARY KEY(Correspondence_ID),
	FOREIGN KEY(Case_ID) REFERENCES patentCase(Case_ID)
);
USE CMS
GO

CREATE TRIGGER docketDueDate--trigger to auto generate due date for the correspondence types that require actions
ON correspondence
AFTER INSERT
AS 
BEGIN
	SET NOCOUNT ON
	UPDATE correspondence SET DueDate = DATEADD(MONTH, 3, i.MLD)
	FROM correspondence c
	JOIN inserted i on c.Correspondence_ID = i.Correspondence_ID
	WHERE i.Correspondence_type IN ('Regular Office Action', 'Final Office Action', 'Advisory Action', 'Notice on Appeal')
END
;
--using bulk insert to import the data to the tables

BULK 
INSERT client
FROM 'C:\Users\client.txt'
WITH
(
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '\n',
	FIRSTROW = 2
);
BULK 
INSERT associate
FROM 'C:\Users\associate.txt'
WITH
(
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '\n',
	FIRSTROW = 2
);
BULK 
INSERT patentCase
FROM 'C:\Users\patentCase.txt'
WITH
(
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '\n',
	FIRSTROW = 2
);
BULK 
INSERT correspondence
FROM 'C:\Users\correspondence.txt'
WITH
(	FIRE_TRIGGERS,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '\n',
	FIRSTROW = 2
);

GO
--number of rejections received per case

SELECT t.Case_ID, sum(t.Rej_counts) as Total_Rej
FROM
  (SELECT *,
  CASE	
	WHEN Correspondence_type IN ('Regular Office Action',
	'Notice on Appeal', 'Advisory Action','Final Office Action') THEN 1
	ELSE 0
	END AS Rej_counts
  from
  [correspondence])t
  group by t.Case_ID;

--number of rejections received per attorney
SELECT p.WA_ID,
	ROUND(CAST(COUNT(*) AS FLOAT)/COUNT(DISTINCT c.Case_ID),2) AS AvgRejCount	
FROM correspondence c
JOIN patentCase p
ON c.Case_ID = p.Case_ID
WHERE c.Correspondence_type IN ('Regular Office Action',
	'Notice on Appeal', 'Advisory Action','Final Office Action')
GROUP BY p.WA_ID
ORDER BY AvgRejCount
--abandoned cases by attorney
SELECT WA_ID, count(*) AS abandoned_cases
FROM patentCase c
WHERE c.Status = 'abandoned'
GROUP BY WA_ID

--issued cases by attorney

SELECT WA_ID, count(*) AS issued_cases
FROM patentCase c
WHERE c.Status = 'granted'
GROUP BY WA_ID

--total cases by attorney
SELECT WA_ID, CAST(count(*) AS FLOAT) as total_cases
FROM patentCase c
GROUP BY WA_ID

--abandon rate
SELECT t.WA_ID, ROUND(t2.abandoned_cases/t.total_cases,2) AS abandon_rate
FROM
(SELECT WA_ID, CAST(count(*) AS FLOAT) as total_cases
FROM patentCase c
GROUP BY WA_ID) t
JOIN 
(SELECT WA_ID, count(*) AS abandoned_cases
FROM patentCase c
WHERE c.Status = 'abandoned'
GROUP BY WA_ID) t2
ON t.WA_ID = t2.WA_ID
ORDER BY abandon_rate

--granted rate
SELECT t.WA_ID, ROUND(t2.issued_cases/t.total_cases,2) as granted_rate
FROM
(SELECT WA_ID, CAST(count(*) AS FLOAT) as total_cases
FROM patentCase c
GROUP BY WA_ID) t
JOIN
(SELECT WA_ID, count(*) AS issued_cases
FROM patentCase c
WHERE c.Status = 'granted'
GROUP BY WA_ID) t2
ON t.WA_ID = t2.WA_ID
ORDER BY granted_rate desc

--avg time it takes each assocaite's cases from filing to granted
SELECT t.WA_ID, a.First_name, a.Last_name,
	t.avg_months
FROM(
SELECT t.WA_ID, ROUND(AVG(t.months),2) AS avg_months
FROM(
SELECT *, DATEDIFF(MONTH, c.Filing_date, c.Issue_date) as months
FROM patentCase c
WHERE c.Status = 'granted') t
GROUP BY t.WA_ID)t
JOIN associate a
ON t.WA_ID = a.Emp_ID
ORDER BY T.avg_months 

--numbers of unfiled and pending case to evulate the future workload
SELECT c.WA_ID, COUNT(*) AS num_pending_cases
FROM patentCase c
WHERE c.Status IN ('unfiled', 'pending')
GROUP BY c.WA_ID
ORDER BY num_pending_cases DESC



--create view to connect all tables for using in Tableau
CREATE VIEW analysis as(
SELECT c.Correspondence_ID,
	c.Case_ID,
	c.Correspondence_type,
	c.MLD,
	c.DueDate,
	p.Status,
	p.Filing_date,
	p.Issue_date,
	p.WA_ID,
	a.Rate,
	CONCAT(a.First_name,' ',a.Last_name) AS AttyName,
	cl.Client_ID,
	cl.Name,
	cl.Billing_atty,
	CONCAT(a1.First_name,' ',a1.Last_name) AS BillingAttyName
FROM patentCase p
JOIN correspondence c
ON p.Case_ID = c.Case_ID
JOIN client cl
ON p.Client_ID = cl.Client_ID
JOIN associate a
ON p.WA_ID = a.Emp_ID
JOIN associate a1
ON a1.Emp_ID=cl.Billing_atty)

--create table function to 
CREATE FUNCTION udfPerformancePerClt (@clt varchar(20))
RETURNS TABLE
AS 
RETURN
	(
		SELECT t1.AttyName, t1.AvgRejCount, t2.avg_months
FROM(
SELECT AttyName,
	ROUND(CAST(COUNT(*) AS FLOAT)/COUNT(DISTINCT Case_ID),2) AS AvgRejCount
FROM analysis 
WHERE Name = @clt
	AND Correspondence_type IN ('Regular Office Action',
	'Notice on Appeal', 'Advisory Action','Final Office Action') 
GROUP BY AttyName) t1
JOIN (
SELECT t.AttyName,ROUND(AVG(t.months),2) AS avg_months
FROM(
SELECT AttyName, DATEDIFF(MONTH, Filing_date, Issue_date) as months
FROM analysis
WHERE Status = 'granted' AND Name = @clt) t
GROUP BY T.AttyName) t2
ON t1.AttyName = t2.AttyName 
	)
SELECT * 
FROM udfPerformancePerClt ('Apple')
ORDER BY AvgRejCount