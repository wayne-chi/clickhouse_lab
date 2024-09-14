SHOW CREATE crypto_prices; 


SELECT COUNT ()
FROM crypto_prices
WHERE volume >= 1000_000;

/*

Define a new table named crypto_prices that satisfies the following requirements:

a. Use the column names and data types above, except: 

i. do not use Nullable on any columns

ii. change trade_date to a Date

iii. use LowCardinality for the crypto_name

b. The table engine is MergeTree

c. The primary key is the crypto_name followed by the trade_date

*/

CREATE TABLE default.crypto_prices2
(
    `trade_date` UInt16,
    `crypto_name` String,
    `volume` Float32,
    `price` Float32,
    `market_cap` Float32,
    `change_1_day` Float32
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name , trade_date)
SETTINGS index_granularity = 8192;

/* 
Insert all of the data from the parquet file in S3 into your new crypto_prices table.
*/
INSERT INTO crypto_prices2
select * FROM crypto_prices;

/*
What is the average price of all trades where the name of the cryptocurrency name starts with a capital 'B'? 
You can use the standard LIKE from SQL. Notice how many granules were processed. 
Clearly the primary index was also useful in this query.
*/
SELECT avg(price) from crypto_prices2
WHERE crypto_name LIKE 'B%';


SHOW TABLES in system;

SELECT * from system.tables;
SELECT * FROM system.server_settings;

