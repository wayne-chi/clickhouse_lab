/*
Run the following query, 
which returns the amount of disk space used by your uk_price_paid table, 
along with the uncompressed size and the number of parts:
*/
SELECT
    formatReadableSize(sum(data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio,
    count() AS num_of_parts
FROM system.parts
WHERE table = 'uk_price_paid' AND active = 1;


/*
Notice that the default compression (LZ4) did a pretty good job of compressing this data - basically 3.5 times. 
That was the overall compression of the entire table. 
Run the following query, which shows the compression of each column:
*/
SELECT
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'uk_price_paid' AND active = 1
GROUP BY column;

/*
Notice the output doesn't seem very helpful - all 0.00 bytes! What is happening here?

ClickHouse uses columnar storage - it stores the data of each column in a separate file. 
This is referred to as the Wide format of a MergeTree table. 
For smaller tables, ClickHouse simply stores all the data in a single file - referred to as the Compact format. 
Your uk_price_paid table is small enough that ClickHouse Cloud is storing it in the Compact format, 
so attempting to view the per-column compression doesn't make sense - there are no separate column files.
*/

/*
Define the following table named prices_1 and insert all of the rows from uk_price_paid into prices_1. 
Notice that prices_1 sets min_rows_for_wide_part to 0 and min_bytes_for_wide_part to 0, 
which basically forces ClickHouse to store the table in the wide format.
*/
CREATE TABLE prices_1
(
    `price` UInt32,
    `date` Date,
    `postcode1` LowCardinality(String) ,
    `postcode2` LowCardinality(String),
    `type` Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    `is_new` UInt8,
    `duration` Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    `addr1` String,
    `addr2` String,
    `street` LowCardinality(String),
    `locality` LowCardinality(String),
    `town` LowCardinality(String),
    `district` LowCardinality(String),
    `county` LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, date)
SETTINGS min_rows_for_wide_part=0,min_bytes_for_wide_part=0;

INSERT INTO prices_1
    SELECT * FROM uk_price_paid;

/*
Run the query from Step 2 above again, but this time run it on the prices_1 table.
*/
SELECT
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'prices_1' AND active = 1
GROUP BY column;

/*
Notice postcode1 has a compression of ~600 times, 
and town has a compression of ~240 times - which is amazing. 
Why are those two columns so nicely compressed?

7
Notice the worst compression is date and price. 
Why do you think those two columns did not compress well?
*/

/* 8
You're about to find out how good the default settings are in ClickHouse by trying to improve on them by configuring your own codecs. 
Define a new MergeTree table named prices_2 that contains the following five columns:
a. price as a UInt32 compressed using T64 and LZ4
b. date as a Date compressed using DoubleDelta and ZSTD, and make date the primary key
c. postcode1 and postcode2 as just String columns (no codecs and no LowCardinality)
d. is_new as a UInt8 compressed using LZ4HC
e. Configure your table similar to prices_1 so that ClickHouse uses the wide format
*/

CREATE OR REPLACE TABLE prices_2
(
    `price` UInt32  CODEC(T64,LZ4),
    `date` Date  CODEC(DoubleDelta, LZ4),
    `postcode1` String ,
    `postcode2` String,
    `is_new` UInt8 CODEC(LZ4HC)
)
ENGINE = MergeTree
PRIMARY KEY date
SETTINGS min_rows_for_wide_part=0,min_bytes_for_wide_part=0;;
/*
Populate prices_2 using the following query:
*/
INSERT INTO prices_2
    SELECT price, date, postcode1, postcode2, is_new FROM uk_price_paid;


/*
Run the same query from steps 2 and 5 again, but this time on the prices_2 table. 
How did the new codecs work out? Why is date compressed so much more this time around?
*/
SELECT
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'prices_2' AND active = 1
GROUP BY column;



