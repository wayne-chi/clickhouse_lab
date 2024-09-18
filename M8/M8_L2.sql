/*
Suppose you have a dashboard with several visualizations that need to be updated on a regular basis. 
Feel free to run the following queries to see what the results are:
a. The maximum and minimum price of properties sold each month:
*/

WITH
    toStartOfMonth(date) AS month
SELECT 
    month,
    min(price) AS min_price,
    max(price) AS max_price
FROM uk_price_paid
GROUP BY month 
ORDER BY month DESC;

-- b The average price of homes sold each month:
WITH
    toStartOfMonth(date) AS month
SELECT 
    month,
    avg(price)
FROM uk_price_paid
GROUP BY month 
ORDER BY month DESC;

-- C The volume (the number of properties) sold each month
WITH
    toStartOfMonth(date) AS month
SELECT 
    month,
    count()
FROM uk_price_paid
GROUP BY month 
ORDER BY month DESC;

/*
In ClickHouse, it's a best practice to minimize the number of materialized views on a table. 
Define a single materialized view that computes and maintains all of the aggregations in step 1 above. Here are some guidelines:
a. Name your destination table uk_prices_aggs_dest
b. Name your materialized view uk_prices_aggs_view
c. Populate the destination table with all the existing rows in uk_price_paid where the date is before January 1, 2024. 
(This will avoid those few sample rows that you may have added in a previous lab.)
*/

CREATE TABLE uk_prices_aggs_dest
(
month Date,
min_price SimpleAggregateFunction(min,UInt32), 
max_price SimpleAggregateFunction(max,UInt32),
avg_price AggregateFunction(avg, UInt32),
count AggregateFunction(count, UInt32)
)
ENGINE = AggregatingMergeTree
PRIMARY KEY month;


CREATE MATERIALIZED VIEW uk_prices_aggs_view
TO uk_prices_aggs_dest
AS
SELECT 
    toStartOfMonth(date) AS month,
    minSimpleState(price) AS min_price,
    maxSimpleState(price) AS max_price,
    avgState(price) as avg_price,
    countState() as count
FROM uk_price_paid
GROUP BY month ;

INSERT INTO uk_prices_aggs_dest
SELECT 
    toStartOfMonth(date) AS month,
    minSimpleState(price) AS min_price,
    maxSimpleState(price) AS max_price,
    avgState(price) as avg_price,
    countState() as count
FROM uk_price_paid
WHERE toStartOfMonth(date) < toDate('2024-01-01')
GROUP BY month 
ORDER BY month DESC;

/*&
Select all the rows from uk_prices_agg_dest. 
Notice the "simple" aggregate functions have a readable value, 
and the other aggregate functions contain binary data:
*/
SELECT * FROM uk_prices_aggs_dest;

-- DROP TABLE uk_prices_aggs_view;

/*
Using the destination table, write a query that returns the minimum and maximum price for each of the last 12 months.
*/
SELECT 
    month,
    min(min_price) as min_m_price,
    max(max_price) as max_m_price
FROM uk_prices_aggs_dest
GROUP BY month
ORDER BY month DESC LIMIT 12; 

/*
Similarly, write a query on the destination table that returns the average price of homes for the last two years.
*/

SELECT 
    month,
    avgMerge(avg_price) as avg_m_price
FROM uk_prices_aggs_dest
GROUP BY month
ORDER BY month DESC LIMIT 24;

/*
Write a query on the destination table that computes the number of homes sold in 2020.
*/
SELECT

    countMerge(count)
FROM uk_prices_aggs_dest
WHERE toYear(month) = '2020'
GROUP BY toYear(month);

/*
Let's verify your view is triggered on inserts. Insert the following test rows into uk_price_paid:
*/
INSERT INTO uk_price_paid (date, price, town) VALUES
    ('2024-08-01', 10000, 'Little Whinging'),
    ('2024-08-01', 1, 'Little Whinging');

/*
You should see a new row in uk_prices_aggs_dest for the month of August, 2024:
*/
SELECT 
    month,
    min(min_price),
    max(max_price)
FROM uk_prices_aggs_dest
WHERE toYYYYMM(month) = '202408'
GROUP BY month;


