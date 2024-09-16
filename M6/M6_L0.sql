/*
Materialized view are second tables that  have the data stored with a new order or primary key
and has an insert trigger on the original table.
so when new values are inserted to the original table there are also inserted to the new table.
Note: on delete and modification it doesnt reflect on materialized view
*/
-- defining a materialized view
CREATE MATERIALIZED VIEW uk_prices_by_town
ENGINE = MergeTree
ORDER BY town 
POPULATE AS
SELECT price, 
date, street,
town, district
FROM uk_price_paid;
/*
note : when using the populate , the data is inserted before the insert trigger is set
so any data inserted into the main table during insert/populate of the mv, will be missed.

a work around is to first create the mv without a data, then insert all those rows that where present before its creation.
A 3 way step to achieve this is 
- create the destination table
- define the mv using the TO clause "to" the destination table
- populate the destination table with historic data
*/
--Step 1 : create destination table
CREATE TABLE uk_prices_by_town_dest(
    price UInt32,
    date Date,
    street LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY town;

-- step 2 define materialized VIEW
CREATE MATERIALIZED VIEW uk_prices_by_town_view
TO uk_prices_by_town_dest
AS SELECT
price, date, street, town, district
FROM uk_price_paid
where date >= toDate('2024-09-16 11:45:00');
-- note that s current date and time offset to the fuuture by a little bit of time

-- step 3 ppopulate the historic data
INSERT INTO uk_prices_by_town_dest
SELECT
price, date, street, town, district
FROM uk_price_paid
where date < toDate('2024-09-16 11:45:00');

SELECT now();
SELECT today();
