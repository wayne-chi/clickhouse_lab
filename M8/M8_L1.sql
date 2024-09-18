
/* 1
Let's see what is in the system.clusters table, 
which contains details about your clusters, servers, shards and replicas. 
Run the following query:
*/

SELECT 
    cluster,
    shard_num,
    replica_num,
    database_shard_name,
    database_replica_name
FROM system.clusters;


SELECT event_time, query
FROM system.query_log
ORDER BY event_time DESC
LIMIT 20;

SELECT * FROM system.query_log;

/* Use the clusterAllReplicas function to invoke the query from step 2 on all nodes in the default cluster.*/
SELECT event_time, tables,query
FROM clusterAllReplicas ('default',system.query_log)
ORDER BY event_time DESC
LIMIT 20;

/* 
Write a query that returns all queries executed on the default.uk_price_paid table. 
(Check out what's in the tables column in the system.query_log table.)
*/
SELECT * FROM system.query_log;

SELECT event_time, tables,query
FROM clusterAllReplicas ('default',system.query_log)
where has(tables,'default.uk_price_paid')
ORDER BY event_time DESC;


/* 6
Calculate the number of queries executed on the default cluster that contain the substring 'insert' (case insensitive).
*/

SELECT count()
FROM clusterAllReplicas ('default',system.query_log)
where positionCaseInsensitive(query,'INSERT ') > 0;


/*
Run the following query, which counts the number of parts on whichever node handles the request:
*/
SELECT count()
FROM system.parts;

/*
Now write a query that returns the number of all parts in your default cluster.
*/
SELECT count() FROM clusterAllReplicas ('default', system.parts);

/*
Here's a bonus query for you that demonstrates two nice capabilities. Run the query and notice what it does:
a. It shows details about how much memory your primary indexes are consuming on each instance in a cluster, and
b. It demonstrates how to apply formatReadableSize to some of the columns in the response of a subquery.
*/

SELECT
    instance,
    * EXCEPT instance APPLY formatReadableSize
FROM (
    SELECT
        hostname() AS instance,
        sum(primary_key_size),
        sum(primary_key_bytes_in_memory),
        sum(primary_key_bytes_in_memory_allocated)
    FROM clusterAllReplicas(default, system.parts)
    GROUP BY instance
);




