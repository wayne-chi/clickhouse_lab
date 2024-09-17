
/*
Run the following query, which groups the uk_price_paid by town and sums the price column:
*/
SELECT 
    town,
    sum(price) AS sum_price,
    formatReadableQuantity(sum_price)
FROM uk_price_paid
GROUP BY town
ORDER BY sum_price DESC;

/*
If you want to keep a running total, SummingMergeTree is the perfect solution. 
Create a materialized view that keeps a running sum of the price column for each town in uk_price_paid:
a. Name the view prices_sum_view
b. Name the destination table prices_sum_dest
c. Populate prices_sum_dest with the existing rows in uk_price_paid
*/

CREATE TABLE prices_sum_dest
(
town String,
sum_price SimpleAggregateFunction(sum, UInt64)
)
ENGINE = SummingMergeTree
PRIMARY KEY town;
-- DROP TABLE prices_sum_view;

CREATE MATERIALIZED VIEW prices_sum_view
TO prices_sum_dest
AS SELECT
    town,
    sumSimpleState(price) as sum_price
FROM uk_price_paid
GROUP BY town;


INSERT INTO prices_sum_dest
SELECT
town,
SUMSimpleState(price)
FROM uk_price_paid
GROUP BY town;

/* Q3
Check the rows in prices_sum_dest - you should have 1,172 (one for each town).
*/
SELECT COUNT() from prices_sum_view;

/*
Verify it worked by running the following two queries 
- you should get the same result, but the query reading from prices_sum_dest should be much faster:
*/

SELECT 
    town,
    sum(price) AS sum_price,
    formatReadableQuantity(sum_price)
FROM uk_price_paid
WHERE town = 'LONDON'
GROUP BY town;

SELECT
    town,
    sum(sum_price) AS sum,
    formatReadableQuantity(sum)
FROM prices_sum_dest
WHERE town = 'LONDON'
GROUP BY town;

/*
Do you see a problem with the second query?
 What happens if you insert the sale of a new property in London as below and re-run the queries?
*/

INSERT INTO uk_price_paid (price, date, town, street)
VALUES
    (4294967295, toDate('2024-01-01'), 'LONDON', 'My Street1');


/*
Write a query on prices_sum_dest that returns the top 10 towns in terms of total price spent on property. 
Remember that when you query a SummingMergeTree, there might be multiple rows with the same primary key 
that should be aggregated (i.e., you should always have the sum and the GROUP BY in the query).
*/
SELECT
    town,
    sum(sum_price) AS sum,
    formatReadableQuantity(sum)
FROM prices_sum_dest
GROUP BY town
ORDER BY 2 DESC LIMIT 10;









