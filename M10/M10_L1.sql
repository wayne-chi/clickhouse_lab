-- SELECT * FROM system.parts;
/*
The mortgage rates are in a CSV file that looks like the following:
date,variable,fixed,bank
29/02/2004,5.02,4.9,4
31/03/2004,5.11,4.91,4
30/04/2004,5.07,4.92,4
31/05/2004,5.11,4.92,4.25
1
Create a table named rates_monthly that satisfies the following requirements:
a. The first column is a Date named month
b. The variable, fixed, and bank columns are Decimal32(2)
c. The table engine is ReplacingMergeTree
d. Each month will have a single row of interest rates, so make month the primary key
*/

CREATE TABLE rates_monthly
( month Date,
variable Decimal32(2),
fixed Decimal32(2),
bank Decimal32(2)
)
ENGINE ReplacingMergeTree
primary KEY month;


/*
Run the following command to populate your rates_monthly table. 
(If you are using ClickHouse OSS, use parseDateTime(date, '%d/%m/%Y') instead of toDate(date). 

INSERT INTO rates_monthly
    SELECT
        parseDateTime(date, '%d/%m/%Y') AS month,
        variable,
        fixed,
        bank
    FROM s3(
        'https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv',
        'CSVWithNames');
*/

INSERT INTO rates_monthly
    SELECT
        -- parseDateTime(date, '%d/%m/%Y') 
        toStartOfMonth(date)AS month,
        variable,
        fixed,
        bank
    FROM s3(
        'https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv',
        'CSVWithNames');

SELECT * FROM rates_monthly;


/* 4
View the interest rates for May, 2022:
*/
SELECT * 
FROM rates_monthly 
WHERE month = '2022-05-01';

/*
Change the interest rates for 2022-05-31 to 3.2, 3.0, and 1.1.
*/
ALTER TABLE rates_monthly
UPDATE variable = 3.20, fixed = 3.0 , bank = 1.1
WHERE month = '2022-05-01';


SELECT * 
FROM rates_monthly  FINAL
WHERE month = '2022-05-01';


/*
Let's see how the version column is used in a ReplacingMergeTree table. 
Create a new table rates_monthly2 that is identical to rates_monthly but includes a version column of type UInt32.
*/

CREATE TABLE rates_monthly2
( month Date,
variable Decimal32(2),
fixed Decimal32(2),
bank Decimal32(2),
version UInt32
)
ENGINE ReplacingMergeTree(version)
primary KEY month;

INSERT INTO rates_monthly2(month,variable, fixed, bank, version)
SELECT month, variable, fixed, bank, 1
FROM  rates_monthly;

/*
Insert the following two rows, in the following order, into rates_monthly2:
*/
INSERT INTO rates_monthly2 VALUES 
    ('2022-04-01', 3.1, 2.6, 1.1, 10);

INSERT INTO rates_monthly2 VALUES 
    ('2022-04-01', 2.9, 2.4, 0.9, 5);



SELECT * 
FROM rates_monthly2 
WHERE month = '2022-04-01';

SELECT * 
FROM rates_monthly2 FINAL
WHERE month = '2022-04-01';

/*
Force a merge and see what happens when you run the previous query without FINAL. 
Keep in mind that this is not something done in any practical scenario in ClickHouse - 
you do not force merges and there is no guarantee that your parts will even merge. 
It's just for demonstration purposes. 
Notice only row one exists in the table where month is equal to 2022-04-01.
*/

OPTIMIZE TABLE rates_monthly2 FINAL ;--DEDUPLICATE BY * EXCEPT value;

SELECT * 
FROM rates_monthly2 
WHERE month = '2022-04-01';




