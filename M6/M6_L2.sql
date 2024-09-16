/*
Write a single query on the uk_price_paid table that computes the number of properties sold 
and the average price of all the properties sold 
for the year 2020. Notice your query needs to process all the rows in the table.
*/
SELECT count(), avg(price) 
from uk_price_paid 
where toYear(date) ='2020';

/* Q2
Write a similar query as the one you wrote in step 1, 
except this time return the year, count and average for all the years in the dataset. 
(In other words, group the result by toYear(date) instead of filtering by the year 2020). 
Again, your query will need to process all the rows in the table.
*/
SELECT toYear(date) as year,
COUNT() ,
avg(price)
FROM uk_price_paid
GROUP BY year;

/* Q3
Suppose you want to run queries frequently on the yearly historical data of uk_price_paid. 
Let's define a materialized view that partitions the data by year and sorts the data by town, 
so that our queries do not need to scan every row each time we run our queries. 
Let's start by defining the destination table. Define a new MergeTree table that satisfies the following requirements:
a. The name of the table is prices_by_year_dest
b. The table will store the date, price, addr1, addr2, street, town, district and county columns from uk_price_paid
c. The primary key is the town column followed by the date column
d. The table is partitioned by year
*/
CREATE TABLE prices_by_year_dest
(
date Date,
price UInt32,
addr1 String,
addr2 String,
street LowCardinality(String),
town LowCardinality(String),
district LowCardinality(String),
county LowCardinality(String)
)
ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (town , date);

/*
Create a materialized view named prices_by_year_view that sends the 
date, price, addr1, addr2, street, town, district and county columns to the prices_by_year_dest table.
*/
CREATE MATERIALIZED VIEW prices_by_year_view
TO prices_by_year_dest
AS SELECT
date, price, addr1,addr2, street, town, district, county
FROM uk_price_paid
where date >= toDate('2024-09-16 11:52:00');


-- insert the historic data
INSERT INTO prices_by_year_dest
SELECT
date, price, addr1,addr2, street, town, district, county
FROM uk_price_paid
where date < toDate('2024-09-16 11:52:00');

/*
Count the number of rows in prices_by_year_dest and verify it's the same number of rows in uk_price_paid - 28,634,236 rows.
*/
SELECT COUNT() from prices_by_year_dest;

/*
Run the following query, which returns the parts that were created for your prices_by_year_dest table. 
You will see lots of parts, and folder names contain the year:
*/
SELECT * FROM system.parts
WHERE table='prices_by_year_dest';

SELECT * FROM system.parts
WHERE table='uk_price_paid';

/*
Notice that partitioning by year created a lot of parts. At a 
minimum, you need at least one part for each year from 1995 
to 2023, but it is possible that some of those years have 
multiple part folders. This is a cautionary tale about 
partitioning! Be careful with it - especially when you only have 28M rows. T
here is really no need for us to partition this dataset by year except for educational purposes. 
Only for very large datasets do we recommend partitioning, 
in which case partitioning by month is recommended.

*/

/*
Let's see if we gained any benefits from defining this materialized view. 
Run the same query from step 1, except this time run it on prices_by_year_dest instead of uk_prices_paid. 
How many rows were scanned?
*/
SELECT count(), avg(price) 
from prices_by_year_dest 
where toYear(date) ='2020';
-- 892,756 ROWS scanned


/*
Use prices_by_year_dest to count how many properties were sold and 
the maximum, average, and 90th quantile of the price of properties sold 
in June of 2005 in the county of Staffordshire.

*/
SELECT
COUNT(), MAX(price), AVG(price), quantile(0.9)(price)
from prices_by_year_dest
WHERE toYYYYMM(date)='200506' and upper(county) = 'STAFFORDSHIRE'
FORMAT vertical
;
SELECT county from prices_by_year_dest Where toYear(date)=='2005';
/*
Let's verify that the insert trigger for your materialized view is working properly. 
Run the following command, which inserts 3 rows into uk_price_paid for properties in the year 2024. 
(Right now your uk_price_paid table doesn't contain any transactions from 2024.)
*/

INSERT INTO uk_price_paid VALUES
    (125000, '2024-10-07', 'B77', '4JT', 'semi-detached', 0, 'freehold', 10,'',	'CRIGDON','WILNECOTE','TAMWORTH','TAMWORTH','STAFFORDSHIRE'),
    (440000000, '2024-10-29', 'WC1B', '4JB', 'other', 0, 'freehold', 'VICTORIA HOUSE', '', 'SOUTHAMPTON ROW', '','LONDON','CAMDEN', 'GREATER LONDON'),
    (2000000, '2024-10-22','BS40', '5QL', 'detached', 0, 'freehold', 'WEBBSBROOK HOUSE','', 'SILVER STREET', 'WRINGTON', 'BRISTOL', 'NORTH SOMERSET', 'NORTH SOMERSET');


SELECT* FROM prices_by_year_dest
WHERE toYYYYMM(date) >= '202409';



SELECT * FROM system.parts
WHERE table='prices_by_year_dest';


