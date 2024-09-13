SELECT 
   count() AS count,
   by
FROM hackernews
GROUP BY by
ORDER BY count DESC;
