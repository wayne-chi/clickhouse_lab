/*
Define a view on the uk_price_paid table that satisfies the following requirements:
a. The name of the view is london_properties_view 
b. The view only returns properties in the town of London
c. The view only returns the date, price, addr1, addr2 and street columns
*/
CREATE VIEW london_properties_view
AS
SELECT date, price, addr1, addr2, street 
from uk_price_paid
where town = 'LONDON';

/* Q2
Using your view, compute the average price of properties sold in London in 2022.
*/
SELECT avg(price) from london_properties_view;

/* Q3
Count the number of rows in the view (which should be the number of properties sold in London)
*/
SELECT COUNT() from london_properties_view;
/* Q4
Run the following query, which also counts the number of properties sold in London. You should get the same result on your previous query
*/
SELECT count() 
FROM uk_price_paid
WHERE town = 'LONDON';

/*
Create a parameterized view named properties_by_town_view that is identical to your london_properties_view, 
except instead of filtering by the town equal to 'LONDON', the value of town is defined as a parameter named town_filter.
*/
CREATE VIEW properties_by_town_view
AS
SELECT date, price, addr1, addr2, street 
from uk_price_paid
where town = {town_filter: String};


SELECT avg(price) from properties_by_town_view(town_filter='LONDON');

/*
Write a query using your properties_by_town_view that returns the most expensive property sold in Liverpool, 
along with the name of the street that the property is on. You should get back a 300M pound property on Sefton Street.
*/
SELECT max(price),argMax(street,price) from properties_by_town_view(town_filter ='LIVERPOOL');
