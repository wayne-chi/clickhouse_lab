/*
Write a query that lists all the distinct values of county in the uk_price_paid table. 
Notice there are only 133 unique values.
*/
select DISTINCT(county) from uk_price_paid;
/* 2
The county column is not in the primary key of uk_price_paid, 
so filtering by county requires an entire table scan, 
as you can see by running the following query
*/
SELECT
    formatReadableQuantity(count()),
    avg(price)
FROM uk_price_paid
WHERE county = 'GREATER LONDON' AND date < toDate('2024-01-01');

/*
This seems like a good scenario for a skipping index. 
Define a new skipping index named county_index on the uk_price_paid table that satisfies the following requirements:
a. It is a set index on the county column
b. No more than 10 unique values of county will be sorted per block
c. The granularity of the index is 5
*/
ALTER TABLE uk_price_paid
    ADD INDEX county_index county
    TYPE SET(10)
    GRANULARITY 5;
/*
Materialize the county_index. 
*/
ALTER TABLE uk_price_paid
    MATERIALIZE INDEX county_index;
/*
Check the status of the materializing of the new index by monitoring the system.mutations table.
*/
SELECT * from system.mutations;
SELECT *
FROM system.mutations
WHERE table = 'uk_price_paid';


/*
When the mutation is complete, 
run the following query to see the size of data skipping indexes (aka secondary indexes).
Notice that the size of the data skipping indexes is very small (~10KB), 
especially if compared to the dataset. 
Notice that all the other tables have a value of zero, so no data skipping index.
*/

SELECT
    table,
    formatReadableSize(data_compressed_bytes) as data_compressed,
    formatReadableSize(secondary_indices_compressed_bytes) as index_compressed,
    formatReadableSize(primary_key_size) as primary_key
FROM
    system.parts
ORDER BY secondary_indices_uncompressed_bytes DESC
LIMIT 5;

-- Run the query from step 2 again. How many rows were scanned this time?
SELECT
    formatReadableQuantity(count()),
    avg(price)
FROM uk_price_paid
WHERE county = 'GREATER LONDON' AND date < toDate('2024-01-01');

/*
The uk_price_paid table has 28M rows, which is over 3,500 granules. 
Your county_index definitely helped because only 5.3M rows were scanned when running the query again. 
Note that this is about 656 granules, or 19% of the rows in the table.
*/
/*
We can do better! Setting the granularity to 5 was too optimistic. 
It means your set index is trying to skip 8,192 x 5 = 40,560 rows in each block. 
Let's try this again with different values. 
Start by dropping the county_index from uk_price_paid.
*/

ALTER TABLE uk_price_paid
DROP INDEX county_index;

/*
Define county_index again similar to how it was defined before, 
except change the GRANULARITY to 1 (instead of 5).
*/

ALTER TABLE uk_price_paid
    ADD INDEX county_index county
    TYPE set(10)
    GRANULARITY 1;

ALTER TABLE uk_price_paid
    MATERIALIZE INDEX county_index;

/*
Materialize county_index and wait for the mutation to complete.
When the mutation is complete, 
run the following query to see the size of data skipping indexes (aka secondary indexes).
*/
SELECT
    table,
    formatReadableSize(data_compressed_bytes) as data_compressed,
    formatReadableSize(secondary_indices_compressed_bytes) as index_compressed,
    formatReadableSize(primary_key_size) as primary_key
FROM
    system.parts
ORDER BY secondary_indices_uncompressed_bytes DESC
LIMIT 5;

/*
3
Run the query from step 2 again. Notice this time the query only reads 4.2M rows.

14
Run the EXPLAIN command with indexes = 1 on the query from step 2.
This will show you exactly how many granules would be skipped using county_index vs. the primary index (without actually running the query). 
It's a very useful output when you are designing and testing skipping indexes.
*/

EXPLAIN indexes = 1 SELECT
    formatReadableQuantity(count()),
    avg(price)
FROM uk_price_paid
WHERE county = 'GREATER LONDON';















