 /*
 Define a new MergeTree table named ttl_demo that satisfies the following requirements:
a. The primary key is a UInt32 column named key
b. Contains a String column named value
c. Contains a DateTime column named timestamp
d. Rows are deleted after 60 seconds
*/
CREATE TABLE ttl_demo(
    key UInt32,
    value String,
    timestamp DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL timestamp + INTERVAL 60 SECOND;

/*
Insert the following two rows into your ttl_demo table:
*/
INSERT INTO ttl_demo VALUES 
    (1, 'row1', now()),
    (2, 'row2', now());

/*Verify the insert worked: */

SELECT * FROM ttl_demo;

/*
Wait 60 seconds, then select the rows in ttl_demo again. They have not been deleted yet. Why not?

5
Force the rules of ttl_demo using the MATERIALIZE TTL command.
*/
SELECT * FROM ttl_demo;

ALTER TABLE ttl_demo
    MATERIALIZE TTL;

/*
Now view the rows of ttl_demo. The table should be empty now.

7
Alter your ttl_demo table so that the value column is removed after 15 seconds.
*/
SELECT * FROM ttl_demo;
--Step 7:
ALTER TABLE ttl_demo
    MODIFY COLUMN value String TTL timestamp +  INTERVAL 15 SECOND;



-- Insert the same two rows again:

INSERT INTO ttl_demo VALUES 
    (1, 'row1', now()),
    (2, 'row2', now());




-- 12:
ALTER TABLE prices_1
    MODIFY TTL date + INTERVAL 5 YEAR;

/*
Materialize the TTL rules on prices_1 so that your new TTL rule gets applied.
*/b 
ALTER TABLE prices_1
MATERIALIZE TTL;

/* 14
Verify that the table no longer contains rows older than 5 years by selecting the minimum value of the date column. 
You should get back the date that is 5 years ago from today.
*/
SELECT min(date) FROM prices_1;



