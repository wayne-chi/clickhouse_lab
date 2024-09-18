
/*
Use the s3 table function to explore the data.
2
Notice the CSV file has four columns: 
a date column for each month; and three interest rates: the variable rate, the fixed rate, and the bank's rate. 
Define a Dictionary that satisfies the following requirements:
a. The name is uk_mortgage_rates
b. Use DateTime64 for the date column
c. Use Decimal32(2) for variable, fixed and bank columns
d. Use COMPLEX_KEY_HASHED for the  layout
e. Set the lifetime to one month (2628000000 seconds)
*/
CREATE DICTIONARY uk_mortgage_rates
(
    date DateTime64,
    variable Decimal32(2),
    fixed  Decimal32(2),
    bank Decimal32(2)
)
PRIMARY KEY date
SOURCE(
    HTTP(
        url 'https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv'
        format 'CSVWithNames'
    )
 )
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(2628000000) ;

SELECT * FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv');
/*
Check the rows in your dictionary to see if it worked. You should see 220 rows.
*/
SELECT count() from uk_mortgage_rates;

/*
Let's try to find a correlation between the volume of properties sold and the interest rate. 
Write a query that returns the number of properties sold per month along with the variable interest rate for that month. 
You should get back 220 rows - one for each month in the dictionary.
*/

WITH toStartOfMonth(uk_price_paid.date) as month
SELECT 
    month,
    count(price),
    avg(variable)
FROM uk_price_paid 
JOIN uk_mortgage_rates
on month = toStartOfMonth(uk_mortgage_rates.date)
GROUP BY month;

/*
It's not obvious if there is a correlation or not. 
Try running the previous query again, but sort the results by the volume of property sold descending.
*/
WITH toStartOfMonth(uk_price_paid.date) as month
SELECT 
    month,
    count(price),
    avg(variable)
FROM uk_price_paid 
JOIN uk_mortgage_rates
on month = toStartOfMonth(uk_mortgage_rates.date)
GROUP BY month ORDER BY 2 DESC;

/* 6 
It is hard to tell just by looking at the numbers, so let's use a more scientific approach. 
Using the query from step 4 as a subquery, use the corr function on the count() and variable columns returned from that query in step 4. 
(You will have to convert the Decimal32 values to Float32.) 
The result of the corr function is the Pearson correlation coefficient. 
If the response is greater than 0, then the two columns move in the same direction. 
If the response is negative, then the two values move in the opposite direction.
*/

SELECT corr(toFloat32(count), toFloat32(var)) 
FROM (
    WITH toStartOfMonth(uk_price_paid.date) as month
    SELECT 
        month,
        count(price) as count,
        avg(variable) as var
    FROM uk_price_paid 
    JOIN uk_mortgage_rates
    on month = toStartOfMonth(uk_mortgage_rates.date)
    GROUP BY month
)  ;  -- 0.2893552

/*  7
Based on the previous query, there is a relationship between volume of properties sold and the variable interest rate. 
Run that query again, but this time use the fixed interest rate volume (instead of variable).
*/
SELECT corr(toFloat32(count), toFloat32(fixed)) 
FROM (
    WITH toStartOfMonth(uk_price_paid.date) as month
    SELECT 
        month,
        count(price) as count,
        any(fixed) as fixed
    FROM uk_price_paid 
    JOIN uk_mortgage_rates
    on month = toStartOfMonth(uk_mortgage_rates.date)
    GROUP BY month
)  ; 

