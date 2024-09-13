/*
Find all properties that sold for more than 100,000,000 pounds, sorted by descending price.
*/
SELECT * 
FROM uk_price_paid
WHERE price >100_000_000
ORDER BY price DESC;

/*
How many properties were sold for over 1 million pounds in 2022?
*/
SELECT count() 
FROM uk_price_paid_2
WHERE price >1_000_000 and toYear(date) ='2022';

/*
How many unique towns are in the dataset?
*/
SELECT uniqExact(town) FROM uk_price_paid_2;
SELECT uniq(town) FROM uk_price_paid_2;

--Which town had the highest number of properties sold?
SELECT town, count() as c 
from uk_price_paid_2
GROUP BY town 
ORDER BY c DESC 
LIMIT 10; -- answer LONDON

 -- the clickhouse way
SELECT topK(1)(town) from uk_price_paid_2;

/*
Using the topK function, write a query that returns the top 10 towns 
that are not London with the most properties sold.
*/
SELECT arrayJoin(topKIf(10)(town,town != 'LONDON') ) as top10 from uk_price_paid_2;

-- 6 What are the top 10 most expensive towns to buy property in the UK, on average?
SELECT town, avg(price) 
FROM uk_price_paid_2 
GROUP BY town
ORDER BY 2 DESC LIMIT 10;

/* 7
What is the address of the most expensive property in the dataset? 
(Specifically, return the addr1, addr2, street and town columns.)
*/
-- haha a good use of argmax
SELECT 
    argMax(addr1,price) as addr1,
    argMax(addr2 ,price) as addr2,
    argMax(street,price) as street,
    argMax(town,price) as town
FROM uk_price_paid;

/* 8
Write a single query that returns the average price of properties for each type. 
The distinct values of type are detached, semi-detached, terraced, flat, and other.
*/
SELECT type, avg(price)
FROM uk_price_paid_2 
GROUP BY 1;

/* 9
What is the sum of the price of all properties sold in the counties of 
Avon, Essex, Devon, Kent, and Cornwall in the year 2020?
*/
SELECT SUM(price)
FROM uk_price_paid
WHERE lower(county) IN ('avon','essex','devon','kent','cornwall')
AND toYear(date) ='2020';

SELECT SUM(price)
FROM uk_price_paid
WHERE multiSearchAnyCaseInsensitive(county,['Avon','Essex','Devon','Kent','Cornwall']) !=0
AND toYear(date) ='2020';

/*
What is the average price of properties sold per month from 2005 to 2010?
*/
SELECT avg(price), toStartOfMonth(date) as month 
from uk_price_paid
where month BETWEEN makeDate(2005,01) and makeDate(2010,12,30)
GROUP BY month;

select toStartOfYear('2005-01-01');

/*11
How many properties were sold in Liverpool each day in 2020?
*/
SELECT avg(price), toStartOfDay(date) as day 
from uk_price_paid
where toYear(date) = '2020'
GROUP BY day;

/*
Write a query that returns 
the price of the most expensive property in each town 
divided by the price of the most expensive property in the entire dataset. 
Sort the results in descending order of the computed result.
*/

with (
SELECT max(price)  from uk_price_paid ) as most_expensive
select max(price) / most_expensive,
town
from uk_price_paid
GROUP BY town ORDER BY 1 DESC;



-- ------------------------------------------
DESC uk_price_paid_2;
SELECT distinct(county) FROM uk_price_paid;

SELECT makeDate(2005,01);
-- ----------------------------------------






