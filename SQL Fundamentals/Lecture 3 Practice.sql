-- show all content of albums TABLE
select * from albums;
-- show first 10 rows of albums table
/* multiple
lines */
SELECT * from albums limit 10;

-- find all customer names
SELECT FirstName,LastName from customers;
-- find all employees born after 1970
SELECT FirstName,LastName, BirthDate from employees where BirthDate > '1970-01-01';
-- find a person born between 1960 and 1970
-- order from youngest to oldest
select FirstName, LastName, BirthDate from employees where BirthDate between "1960-01-01" and "1970-01-01"
order by BirthDate DESC;
-- find these employees from Calgary
select FirstName, LastName, BirthDate, city from employees where BirthDate between "1960-01-01" and "1970-01-01"
and city == "Calgary" order by BirthDate DESC;
-- count number of employees from Calgary
select count() as "number of employees" from employees where city == "Calgary";
-- find invoices from north america
select BillingCity, BillingCountry from invoices where BillingCountry in ("USA", "Canada", "Mexico");
-- find all artists whose name start with A
select * from artists where name like "A%";
-- find oldest employee 
select FirstName, min(birthdate) as "oldest employee" from employees;
-- find number of invoices from each north american country
select BillingCountry, count(*) as "number of invoices in each country" from invoices where BillingCountry in ("USA", "Canada", "Mexico")
group by BillingCountry;