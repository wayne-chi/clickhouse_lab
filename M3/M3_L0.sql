SELECT * FROM system.data_type_families;

CREATE DATABASE rich;

CREATE TABLE rich.friends(
name String,
birthdate Date,
age UInt8
)
ENGINE = MergeTree
PRIMARY KEY name;

ALTER TABLE rich.friends
    ADD COLUMN meetings Array(DateTime);

SHOW CREATE rich.friends;

CREATE TABLE enum_demo (
    device_id UInt32,
    device_type Enum ('server' =1, 'container'=2, 'router'=3)
)
ENGINE = MergeTree
PRIMARY KEY device_id;

