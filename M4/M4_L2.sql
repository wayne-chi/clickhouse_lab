DESCRIBE s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices.snappy.parquet') 
SETTINGS schema_inference_make_columns_nullable=False;


SELECT count() from s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices.snappy.parquet') ;

SELECT * from s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices.snappy.parquet') LIMIT 10 ;


CREATE TABLE uk_price_paid_2(
price	UInt32,
date	Date,
postcode1	LowCardinality(String),
postcode2	LowCardinality(String),
type	Enum('terraced'=1, 'semi-detached'=2, 'detached'=3,'flat'=4,'other'=0),
is_new	UInt8,
duration	Enum('freehold'=1,'leasehold'=2,'unknown'=0),
addr1	String,
addr2	String,
street	LowCardinality(String),
locality	LowCardinality(String),
town	LowCardinality(String),
district	LowCardinality(String),
)
ENGINE = MergeTree
PRIMARY KEY (postcode1, postcode2, date);


INSERT INTO uk_price_paid_2
SELECT * from s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices.snappy.parquet');

SELECT * from uk_price_paid_2 LIMIT 10;

/* 5FTUsing the avg() function, 
compute the average price of properties sold where postcode1 equals LU1 and postcode2 equals 5FT. 
Notice the query only has to read 8,192 rows (exactly 1 granule!). Why are so few rows processed?
*/
SELECT avg(price) from uk_price_paid_2
where (postcode1 = 'LU1') and (postcode2 ='5FT');

/*  6
Modify the previous query but only filter on rows where postcode2 equals 5FT. 
Notice it doesn't scan every row in the table, but it scans a large number (around 19-20M rows). 
Why did it need to process so many rows?
*/
SELECT avg(price) from uk_price_paid_2
where (postcode2 ='5FT');

/* 7
Similar to the previous query, find the average price of properties sold in the town of YORK. 
Notice every row is read to compute the result. Why are so many rows processed?
*/
SELECT avg(price) from uk_price_paid_2
where town ='YORK';



