-- lab code

show tables FROM system;

SELECT * 
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_0_0.snappy.parquet')
LIMIT 100;

DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_0_0.snappy.parquet');

SELECT 
  toStartOfMonth(TIMESTAMP) AS month_,
  PROJECT,
  COUNT() AS C
  FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_0_0.snappy.parquet')
  GROUP BY month_, PROJECT 
  ORDER BY month_ DESC, C DESC;




SELECT formatReadableQuantity(count())
FROM uk_price_paid;

SELECT formatReadableQuantity(AVG(price)) 
FROM uk_price_paid;
