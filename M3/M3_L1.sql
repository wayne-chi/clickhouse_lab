DESCRIBE pypi;

SELECT uniqExact(COUNTRY) FROM pypi;
SELECT uniqExact (PROJECT), uniqExact(URL) FROM pypi;

CREATE or REPLACE TABLE default.pypi3
(
    TIMESTAMP DateTime,
    COUNTRY_CODE LowCardinality(String),
    URL String,
    PROJECT LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY ( PROJECT, TIMESTAMP);


INSERT INTO pypi3
SELECT * FROM pypi2;


SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table LIKE 'pypi%')
GROUP BY table;

SELECT
    toStartOfMonth(TIMESTAMP) AS month,
    count() AS count
FROM pypi2
WHERE COUNTRY_CODE = 'US'
GROUP BY
    month
ORDER BY
    month ASC,
    count DESC;



