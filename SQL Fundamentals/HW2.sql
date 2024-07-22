-- 1. Get all the columns from the Customers table.
SELECT * FROM customers;
-- 2. Get all Cities from the Customers table.
SELECT City FROM customers;
-- 3. Select all the different values from the Country column in the Customers table.
SELECT DISTINCT Country FROM customers;
-- 4. Select all records where the City column has the value "Berlin"
SELECT * FROM customers WHERE City = "Berlin";
-- 5. Select all records where the CustomerID column has the value 32.
SELECT * FROM customers WHERE CustomerID = 32;
-- 6. Select all records from the Customers table, sort the result alphabetically by the column City.
SELECT * FROM customers ORDER BY City;
-- 7. Select all records from the Customers table, sort the result reversed alphabetically by the column City.
SELECT * FROM customers ORDER BY City DESC;
/* 8. Select all records from the Customers table, sort the result alpabetically, 
first by the column Country, then by the column City. */
SELECT * FROM customers ORDER BY Country, City;
-- 9. Select all records where the City column has the value 'Berlin' and the PostalCode column has the value ‘12209'.
SELECT * FROM customers WHERE City = "Berlin" AND PostalCode = "12209";
-- 10. Select all records where the City column has the value 'Berlin' OR 'London'.
SELECT * FROM customers WHERE City = "Berlin" OR City = "London";
-- 11. Select all records where City is NOT "Berlin"
SELECT * FROM customers WHERE NOT City = "Berlin";
-- 12. Select all records from the Customers where the PostalCode column is NOT empty.
SELECT * FROM customers WHERE PostalCode IS NOT NULL;
-- 13. Select all records where the value of the City column starts with the letter "a".
SELECT * FROM customers WHERE City LIKE "a%";
-- 14. Select all records where the value of the City column ends with the letter "a".
SELECT * FROM customers WHERE City LIKE "%a";
-- 15. Select all records where the value of the City column contains the letter "a".
SELECT * FROM customers WHERE City LIKE "%a%";
/* 16. Select all records where the value of the City column starts with letter "a" and 
ends with the letter “b".*/
SELECT * FROM customers WHERE City LIKE "a%b";
-- 17. Select all records where the value of the City column does NOT start with the letter “a".
SELECT * FROM customers WHERE City NOT LIKE "a%";
-- 18. Select all records where the second letter of the City is an “a".
SELECT * FROM customers WHERE City LIKE "_a%";
-- 19. Select all records where the first letter of the City is an "a" or a "c" or an “s".
SELECT * FROM customers WHERE City LIKE "a%" OR City LIKE "c%" OR City LIKE "s%";
-- 20. Select all records where the first letter of the City starts with anything from an "a" to an “f".
SELECT * FROM customers WHERE City <= "F";
-- 21. Select all records where the first letter of the City is NOT an "a" or a "c" or an “f".
SELECT * FROM customers WHERE City != "a" OR City != "c" OR City != "f";
-- 22. Select all the records where Country is either "Norway" or “France".
SELECT * FROM customers WHERE Country IN("Norway","France");
-- 23. Select all the records where Country is NOT "Norway" and NOT “France".
SELECT * FROM customers WHERE Country NOT IN("Norway","France");
-- 24. List the number of customers in each country.
SELECT Country, COUNT(*) FROM customers GROUP BY Country;
-- 25. List the number of customers in each country, ordered by the country with the most customers first.
SELECT Country, COUNT(*) FROM customers GROUP BY Country ORDER BY COUNT(*) DESC;
-- 26. Select the record with the smallest Price of products.
SELECT * FROM products WHERE Price=(SELECT MIN(Price) FROM products);
-- 27. Return the number of records that have the Price value set to 18.
SELECT COUNT(*) FROM products WHERE Price = 18;
-- 28. Calculate the average Price of all products.
SELECT ProductName, AVG(Price) FROM products GROUP BY ProductName;
-- 29. Calculate the sum of all the Price column values in the Products table.
SELECT SUM(Price) FROM products;
/* 30. Select product names and prices and also display a price type column 
to mark prices higher than 50 as high and price lower than 50 as low.*/
SELECT ProductName, Price, 
CASE 
WHEN Price >= 50 THEN "High"
WHEN Price < 50 THEN "Low"
END AS "Price Type"
FROM products GROUP BY ProductName;