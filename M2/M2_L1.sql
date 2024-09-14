
DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');


SELECT * 
FROM  s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet')
LIMIT 10;


SELECT count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

CREATE TABLE pypi 
(
TIMESTAMP DateTime64,
COUNTRY     String,
URL String,
PROJECT String
)
ENGINE = MergeTree
PRIMARY KEY (TIMESTAMP) ;

INSERT INTO pypi
SELECT 
TIMESTAMP,
COUNTRY_CODE as COUNTRY,
URL,
PROJECT
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

SELECT PROJECT, COUNT(PROJECT) 
FROM pypi 
WHERE toStartOfMonth(TIMESTAMP) = '2023-04-01'   GROUP BY 1 ORDER BY 2 DESC;

SELECT PROJECT, toStartOfMonth(TIMESTAMP)
from pypi LIMIT 10;

SELECT COUNT(PROJECT) 
FROM pypi 
WHERE PROJECT LIKE '%boto%'  ;


CREATE TABLE pypi2 (
    TIMESTAMP DateTime,
    COUNTRY_CODE String,
    URL String,
    PROJECT String 
)
ENGINE = MergeTree
PRIMARY KEY (TIMESTAMP, PROJECT);

INSERT INTO pypi2
    SELECT *
    FROM pypi;

SELECT 
    PROJECT,
    count() AS c
FROM pypi2
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;

SELECT count() FROM pypi2;

CREATE OR REPLACE TABLE pypi2 (
    TIMESTAMP DateTime,
    COUNTRY_CODE String,
    URL String,
    PROJECT String 
)
ENGINE = MergeTree
PRIMARY KEY (PROJECT, TIMESTAMP);

INSERT INTO pypi2
    SELECT *
    FROM pypi;

SELECT 
    PROJECT,
    count() AS c
FROM pypi2
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;

