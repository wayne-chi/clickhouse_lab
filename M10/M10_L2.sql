/*
Define a new CollapsingMergeTree table named messages that satisfies the following requirements:
a. Contains a UInt32 column named id
b. Contains a Date column named day
c. Contains a String column named message
d. Contains a single sign column for use with a CollapsingMergeTree
e. The id is the primary key
*/

CREATE TABLE messages (
    id UInt32,
    day Date,
    message String,
    sign Int8
)
ENGINE = CollapsingMergeTree(sign)
PRIMARY KEY id;

/*
2
*/
INSERT INTO messages VALUES 
   (1, '2024-07-04', 'Hello', 1),
   (2, '2024-07-04', 'Hi', 1),
   (3, '2024-07-04', 'Bonjour', 1);

/*
Verify the insert worked: 
*/

SELECT * FROM messages;

/*
"Update" the row with id equal to 2, setting the day to '2024-07-05' and changing the message to "Goodbye".  

5
"Delete" the row where id equals 3.

6
Verify your new rows were added:

SELECT * FROM messages;
*/

INSERT INTO messages VALUES 
   (2, '2024-07-04', 'Hello', -1),
      (2, '2024-07-05', 'Goodbye', 1),
      (3, '2024-07-04', 'Bonjour', -1);


SELECT * FROM messages ;

SELECT * FROM messages FINAL;

INSERT INTO messages VALUES 
   (1, '2024-07-03', 'Adios', 1);

SELECT * FROM messages FINAL;

/*
The CollapsingMergeTree engine implements an option for frequently deleting and updating data

A mutation operation forces all data parts containing affected rows to be re-written, which can cause considerable I/O and cluster overhead

The ReplacingMergeTree engine implements an efficient way of implementing an upsert
*/
