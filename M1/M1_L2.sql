select  *
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
LIMIT 100;

select  formatReadableQuantity(count()) as C
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet') ;


select avg(volume)
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet') ; 


select (crypto_name) as crypt, avg(volume) as vol
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet') 
GROUP BY 1;


CREATE TABLE crypto_prices
(
    trade_date Date,
    crypto_name String, 
    volume int64,
    price Float64,
    market_cap UInt128,
    change_1_day Float32
)
ENGINE = MergeTree
ORDER BY (trade_date,crypto_name);
