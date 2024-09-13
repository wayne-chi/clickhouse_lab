/*
Inserting an Imperfect file

*/
-- dfdf
/*
There is a file named operating_budget_2022.csv in a bucket in S3 that consists of rows that look like the following:

fiscal_year~service~department~program~description~item_category~approved_amount~recommended_amount~actual_amount~fund~fund_type
2021~SERVING AND SUPPORTING SD~INVESTMENT COUNCIL~INVESTMENT OF STATE FUNDS (3210)~EMPLOYEE SALARIES~PERSONNEL COST~6429955~6213186~5721995.16~INVESTMENT COUNCIL OPERATING~OTHER FUNDS
2022~SERVING AND SUPPORTING SD~REVENUE~COMMISSION ON GAMING - INFO (0293)~CAPITAL OUTLAY~OPERATING EXPENSE~6636~6636~1227.91~SD GAMING COMMISSION FUND~OTHER FUNDS
2022~EDUCATION IN SD~BOARD OF REGENTS~SD SCHOOL FOR THE DEAF (1580)~EMPLOYEE BENEFITS~PERSONNEL COST~424448~442809~200103.29~STATE GENERAL FUND~GENERAL FUNDS
2022~CONTINUED SAFETY~ATTORNEY GENERAL~CRIMINAL INVESTIGATION (2911)~TRAVEL~OPERATING EXPENSE~200238~200238~96474.37~ATTORNEY GENERAL FEDERAL FUNDS~FEDERAL FUNDS
2022~SERVING AND SUPPORTING SD~PUBLIC UTILITIES COMMISSION~PUBLIC UTILITIES COMMISSION (PUC) (2610)~SUPPLIES & MATERIALS~OPERATING EXPENSE~4550~4550~567.87~STATE GENERAL FUND~GENERAL FUNDS
2022~SERVING AND SUPPORTING SD~TRANSPORTATION~GENERAL OPERATIONS (111)~EMPLOYEE SALARIES~PERSONNEL COST~160017~153818~49844.74~RAILROAD ADMINISTRATION FUND~OTHER FUNDS
2022~CONTINUED SAFETY~CORRECTIONS~MIKE DURFEE STATE PRISON (1821)~EMPLOYEE BENEFITS~PERSONNEL COST~0~0~252219.93~FEDERAL STIMULUS FUNDS (COVID-19)~FEDERAL FUNDS
2022~CONTINUED SAFETY~PUBLIC SAFETY~HIGHWAY PATROL (1421)~GRANTS AND SUBSIDIES~OPERATI
*/
-- Load data in with a different delimiter
Desc s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS format_csv_delimiter = '~',
schema_inference_make_columns_nullable=False;

/*
Run a query that uses the s3 table function to read the file and count the number of rows. 
You will need to set the format_csv_delimiter setting to '~'. 
You should get back a count of 6,205 rows.
*/
SELECT count() 
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS format_csv_delimiter='~';
SELECT * 
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS format_csv_delimiter='~';

-- sum of amount 
/* 2
Run a query that sums up the actual_amount column, 
which would represent how much money was actually spent for the year. 
You should get back about $8.16 billion dollars.
*/
SELECT formatReadableQuantity(SUM(actual_amount)),Sum(actual_amount)
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS format_csv_delimiter='~'; 


--
/*
Try to run a query that sums up the approved_amount column, 
which would represent how much money was actually spent for the year. 
What is the issue?
*/
SELECT Sum(approved_amount)
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS format_csv_delimiter='~'; 
-- fails cause it is a String

/*
Run the DESCRIBE command on the table function to view the inferred schema. 
Notice that both the approved_amount and recommended_amount columns are inferred as Nullable(String), 
even though most of the values are numeric dollar amounts.
*/

/*
One clever trick you can use is to cast the string column into a numeric value 
using a function like toUInt32OrZero. 
If one of the values in a row is not a valid integer, 
the function will return 0. 
Write a query that uses toUInt32OrZero to sum up the values
 of both the approved_amount and recommended_amount columns
*/

SELECT Sum(toUInt32OrZero(approved_amount) ) as approved_amount, Sum(toUInt32OrZero(recommended_amount) ) as recommended_amount
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS format_csv_delimiter='~';



/*
The issue with the approved_amount and recommended_amount columns is that a handful of rows contain "n/a" 
instead of a numeric value, so their inferred data type is String. Try running the following query, 
which uses the schema_inference_hints setting and suggests the data type for these two columns to be UInt32
*/

SELECT 
    formatReadableQuantity(sum(approved_amount)),
    formatReadableQuantity(sum(recommended_amount))
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS 
format_csv_delimiter='~',
schema_inference_hints='approved_amount UInt32, recommended_amount UInt32';
/* it doesnt work cause of the n/a */

/*
The schema_inference_hints setting can be a great time saver, 
but for this particular CSV file it is not helping. To really make this work, 
we need to ingest the data into a MergeTree table with proper data types. 
Start by creating a new table named operating_budget that satisfies the following requirements:

a. Uses the MergeTree table engine

b. Contains the following columns as LowCardinality(String):

i. fiscal_year, service, department, program, item_category, and fund

c. Contains a String column for description

d. Contains the following columns as UInt32:

i. approved_amount and recommended_amount

e. Contains a new program_code column as LowCardinality(String). 
This data is derived from the program column as explained later.

f. Contains a Decimal(12,2) column for actual_amount

g. The fund_type is an Enum with three values:

i. GENERAL FUNDS, FEDERAL FUNDS, and OTHER FUNDS

h. The PRIMARY KEY is (fiscal_year, program)

i. Don't forget to set the format_csv_delimiter setting to '~'.
*/

CREATE TABLE operating_budget(
fiscal_year	LowCardinality(String),
service	LowCardinality(String),
department	LowCardinality(String),
program	LowCardinality(String),
program_code LowCardinality(String),
description	String,
item_category	String,
approved_amount	UInt32,
recommended_amount	UInt32,
actual_amount	Decimal(12,2),
fund	LowCardinality(String),
fund_type	Enum('GENERAL FUNDS'=1,'FEDERAL FUNDS'=2, 'OTHER FUNDS'=3)
)
ENGINE = MergeTree
PRIMARY KEY (fiscal_year, program) ;


/*
Move the data to the table, skip the first row
*/
INSERT INTO operating_budget
WITH 
splitByChar('(',col4) as p
SELECT
col1 as fiscal_year,
col2 as service,
col3 as department,
p[1] as program,
trim( TRAILING ')' FROM p[2] ) as program_code,
col5 as description,
col6 as item_category,
toUInt32OrZero(col7) as approved_amount,
toUInt32OrZero(col8) as recommended_amount,
toDecimal64(col9,2) as actual_amount,
col10 as fund,
col11 as fund_type
FROM s3(
    'https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv' ,
    'CSV',
    'col1 String,
    col2 String,
    col3 String,
    col4 String,
    col5 String,
    col6 String,
    col7 String,
    col8 String,
    col9 String,
    col10 String,
    col11 String')
SETTINGS format_csv_delimiter='~',
input_format_csv_skip_first_lines =1;


-- show CREATE TABLE operating_budget;

SELECT * from operating_budget;

/*
Write a query that sums the approved_amount column 
for fiscal_year equal to 2022. 
You should get about $5.09 billion
*/
SELECT SUM(approved_amount) 
FROM operating_budget
where fiscal_year ='2022';

/*
Write a query that sums the actual_amount of money spent in 2022 
for the program_code 031 (the AGRICULTURE & ENVIRONMENTAL SERVICES program).
You should get back $8,058,173.43.
*/
SELECT SUM(actual_amount) 
FROM operating_budget
where program_code ='031';

/*
fiscal_year	String
service	String
department	String
program	String
description	String
item_category	String
approved_amount	String
recommended_amount	String
actual_amount	Float64
fund	String
fund_type	String
*/
