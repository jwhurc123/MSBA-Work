-- 1. Show orders (OrderID and OrderDate) and customers who placed them.
SELECT orders.OrderID, orders.OrderDate, customers.CustomerName 
FROM orders JOIN customers ON orders.CustomerID = customers.CustomerID;
-- 2. Show product and its category
SELECT products.ProductName, categories.CategoryName, categories.Description
FROM products JOIN categories ON products.CategoryID = categories.CategoryID;
-- 3. Show all customers names, and any orders they might have
SELECT customers.CustomerName, orders.OrderID, orders.OrderDate
FROM customers LEFT JOIN orders ON customers.CustomerID = orders.CustomerID;
-- 4. Show all employees, and any orders they might have
SELECT employees.FirstName || " " || employees.LastName AS Employee_Name, orders.OrderID, orders.OrderDate 
FROM employees LEFT JOIN orders ON employees.EmployeeID = orders.EmployeeID;
-- 5. Match all customers that are from the same city (duplicate pairs is fine)
SELECT c1.CustomerName AS Customer_A, c2.CustomerName AS Customer_B, c1.City 
FROM customers AS c1 JOIN customers AS c2 ON c1.City = c2.City 
WHERE Customer_A != Customer_B;
-- 6. Show customers and suppliers that are from the same city
SELECT  customers.City, customers.CustomerName, suppliers.SupplierName
FROM customers JOIN suppliers ON customers.City = suppliers.City;
-- 7a. List the number of customers in each country
SELECT Country, COUNT(*) FROM customers GROUP BY Country;
-- 7b. List the number of customers in each country. Only include countries with more than 5 customers.
SELECT Country, COUNT(*) FROM customers GROUP BY Country HAVING COUNT(*) > 5;
-- 8. Create assigned table in database.
CREATE TABLE students(
	id INTEGER PRIMARY KEY,
	student_data TEXT
);
-- 8a. Insert your JSON object into this table.
INSERT INTO students(student_data) VALUES(
'{
"student_id": "S1001", "first_name": "John", "last_name": "Doe", "age": 20,
"major": "Computer Science", "enrolled_courses": [
{
"course_id": "CS101",
"course_name": "Introduction to Computer Science", "credits": 4
},
{
"course_id": "CS102",
"course_name": "Data Structures and Algorithms", "credits": 4
}
],
"contact_details": {
"email": "john.doe@example.com", "phone": "123-456-7890"
}
}'
);
-- 8b. Show the JSON object in the table.
SELECT json_valid(student_data) FROM students;
-- 8c. Retrieve all students first_name and email and phone number
SELECT json_extract(student_data, '$.first_name') AS first_name,
json_extract(student_data,'$.contact_details.email') AS email,
json_extract(student_data,'$.contact_details.phone') AS phone
FROM students;
-- 8d. Extract all "computer science" students.
SELECT json_extract(student_data, '$.first_name') AS first_name,
json_extract(student_data,'$.contact_details.email') AS email,
json_extract(student_data,'$.contact_details.phone') AS phone,
json_extract(student_data,'$.major') AS major
FROM students WHERE major = "Computer Science";
-- 8e. Display list of courses for each student.
SELECT
students.id, 
json_extract(student_data, '$.first_name') AS first_name,
json_extract(student_data, '$.last_name') AS last_name,
json_extract(value, '$.course_id') AS course_id,
json_extract(value, '$.course_name') AS course_name,
json_extract(value, '$.credits') AS credits
FROM students, json_each(students.student_data,'$.enrolled_courses');
-- 8f. Display first course for all students
SELECT
students.id, 
json_extract(student_data, '$.first_name') AS first_name,
json_extract(student_data, '$.last_name') AS last_name,
json_extract(student_data, '$.enrolled_courses[0].course_id') AS course_id,
json_extract(student_data, '$.enrolled_courses[0].course_name') AS course_name,
json_extract(student_data, '$.enrolled_courses[0].credits') AS credits
FROM students;