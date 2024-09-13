SELECT topK(10)(street) FROM uk_price_paid;

SELECT topKIf(10)(street, street!='') FROM uk_price_paid;

SELECT uniq(street) FROM uk_price_paid;

-- array join expands an array into multiple rows
SELECT 
    arrayJoin(splitByChar(' ',street)),street
FROM uk_price_paid LIMIT 1000;
