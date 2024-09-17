 
 /*
 create aggregating merge tree table
 */
CREATE TABLE some_numbers (
    id UInt8,
    x UInt32
)
ENGINE = MergeTree
PRIMARY KEY id;


CREATE TABLE agg_of_some_numbers
 (
id UInt8,
max_column SimpleAggregateFunction(max, UInt32),
avg_column AggregateFunction(avg, UInt32)
 )
Engine =  AggregatingMergeTree
Primary KEY id ;


CREATE MATERIALIZED VIEW view_of_agg_of_some_numbers
TO agg_of_some_numbers
AS 
    SELECT 
        id,
        maxSimpleState(x) as max_column,
        avgState(x) as avg_column
    FROM some_numbers
    GROUP BY id;

INSERT INTO some_numbers
VALUES
    (1, 10),
    (1, 20),
    (2, 300),
    (2, 400); 

SELECT * from agg_of_some_numbers;

SELECT 
    id,
    max(max_column),
    avgMerge(avg_column) -- note that the avgMerge must correspond to the aggregate function used to make the column
FROM agg_of_some_numbers
GROUP BY id;


INSERT INTO some_numbers
VALUES
    (1, 1000),
    (2, 20);

SELECT 
    id,
    max(max_column),
    avgMerge(avg_column) -- note that the avgMerge must correspond to the aggregate function used to make the column
FROM agg_of_some_numbers
GROUP BY id;

/* Uk aggregated MV */

CREATE TABLE uk_aggregated_prices
(
district String,
avg_price AggregateFunction(avg, UInt32),
max_price SimpleAggregateFunction(max, Float32),
quant90 AggregateFunction(quantiles(0.9), UInt32)
)
ENGINE = AggregatingMergeTree
PRIMARY KEY district;


CREATE MATERIALIZED VIEW uk_aggregated_prices_view
TO uk_aggregated_prices
AS 
    SELECT 
        district,
        avgState(price) as avg_price,
        maxSimpleState(price) as max_price,
        quantilesState(0.9)(price) as quant90
    FROM uk_price_paid 
    GROUP BY district;

SELECT * FROM uk_aggregated_prices; -- empty at this point have to insert data

INSERT INTO uk_aggregated_prices
    SELECT 
        district,
        avgState(price) as avg_price,
        maxSimpleState(price) as max_price,
        quantilesState(0.9)(price) as quant90
    FROM uk_price_paid 
    GROUP BY district;

-- seee the data
SELECT * from uk_aggregated_prices;
-- :Q use the Merge combinator
SELECT 
    district,
    avgMerge(avg_price),
    max(max_price),
    quantilesMerge(0.9)(quant90)
FROM uk_aggregated_prices
GROUP BY district;



