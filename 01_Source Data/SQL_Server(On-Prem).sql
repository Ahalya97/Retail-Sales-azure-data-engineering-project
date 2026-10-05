-- 1. Create Categories Table
CREATE TABLE Categories (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(100),
    inserted_date DATETIME DEFAULT GETDATE(),
    lastmodified_date DATETIME DEFAULT GETDATE()
);
-- 2. Create Products Table
CREATE TABLE Products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(150),
    category_id INT,
    brand VARCHAR(100),
    price DECIMAL(10,2),
    inserted_date DATETIME DEFAULT GETDATE(),
    lastmodified_date DATETIME DEFAULT GETDATE(),

    FOREIGN KEY (category_id) REFERENCES Categories(category_id)
);

-- 3. Create Customers Table (Updated)
CREATE TABLE Customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    middle_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(10),
    age INT,
    city VARCHAR(100),
    state VARCHAR(100),
    signup_date DATE,
    inserted_date DATETIME DEFAULT GETDATE(),
    lastmodified_date DATETIME DEFAULT GETDATE()
);

-- 4. Create Stores Table
CREATE TABLE Stores (
    store_id INT PRIMARY KEY,
    store_name VARCHAR(150),
    city VARCHAR(100),
    state VARCHAR(100),
    inserted_date DATETIME DEFAULT GETDATE(),
    lastmodified_date DATETIME DEFAULT GETDATE()
);

-- 5. Create Sales Transactions Table
CREATE TABLE Sales_Transactions (
    transaction_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    store_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    discount DECIMAL(10,2),
    transaction_date DATETIME,
    payment_method VARCHAR(50),
    last_updated DATETIME DEFAULT GETDATE(),  -- existing column kept as is
    inserted_date DATETIME DEFAULT GETDATE(),
    lastmodified_date DATETIME DEFAULT GETDATE(),

    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id),
    FOREIGN KEY (store_id) REFERENCES Stores(store_id)
);

-- 1. Categories
INSERT INTO Categories (category_id, category_name, inserted_date, lastmodified_date)
VALUES
(1, 'Groceries', DATEADD(DAY, -6, GETDATE()), GETDATE()),
(2, 'Beverages', DATEADD(DAY, -6, GETDATE()), GETDATE()),
(3, 'Snacks', DATEADD(DAY, -6, GETDATE()), GETDATE()),
(4, 'Household', DATEADD(DAY, -6, GETDATE()), GETDATE()),
(5, 'Electronics', DATEADD(DAY, -6, GETDATE()), GETDATE());

-- 2. Products
INSERT INTO Products 
(product_id, product_name, category_id, brand, price, inserted_date, lastmodified_date)
VALUES
(101,'Rice Bag',1,'IndiaGate',1200, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(102,'Milk',2,'Amul',60, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(103,'Biscuits',3,'Britannia',30, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(104,'Detergent',4,'SurfExcel',250, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(105,'Cooking Oil',1,'Fortune',180, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(106,'Sugar',1,'Madhur',50, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(107,'Tea Powder',2,'Tata',120, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(108,'Coffee',2,'Nescafe',200, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(109,'Chips',3,'Lays',20, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(110,'Soap',4,'Dove',40, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(111,'Shampoo',4,'ClinicPlus',120, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(112,'Toothpaste',4,'Colgate',90, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(113,'Butter',2,'Amul',55, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(114,'Curd',2,'Amul',45, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(115,'Juice',2,'Real',80, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(116,'Notebook',4,'Classmate',60, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(117,'Pen',4,'Reynolds',10, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(118,'Laptop',5,'Dell',60000, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(119,'Mobile',5,'Samsung',25000, DATEADD(DAY,-6,GETDATE()), GETDATE()),
(120,'Headphones',5,'Boat',1500, DATEADD(DAY,-6,GETDATE()), GETDATE());

-- 3. Customers 
INSERT INTO Customers
(customer_id, first_name, middle_name, last_name, gender, age, city, state, signup_date, inserted_date, lastmodified_date)
VALUES
(1,'Ravi',NULL,'Kumar','Male',30,'Hyderabad','Telangana','2023-01-01', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(2,'Sita',NULL,'Devi','Female',40,'Vijayawada','AP','2023-01-02', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(3,'John',NULL,'Paul','Male',25,'Bangalore','Karnataka','2023-01-03', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(4,'Anita',NULL,'Sharma','Female',35,'Chennai','TN','2023-01-04', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(5,'Rahul',NULL,'Verma','Male',28,'Delhi','Delhi','2023-01-05', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(6,'Kiran',NULL,'Reddy','Male',32,'Hyderabad','Telangana','2023-01-06', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(7,'Lakshmi',NULL,'Priya','Female',29,'Guntur','AP','2023-01-07', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(8,'Arjun',NULL,'Singh','Male',31,'Mumbai','MH','2023-01-08', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(9,'Pooja',NULL,'Mehta','Female',27,'Pune','MH','2023-01-09', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(10,'Vikram',NULL,'Patel','Male',38,'Ahmedabad','GJ','2023-01-10', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(11,'Neha',NULL,'Gupta','Female',26,'Delhi','Delhi','2023-01-11', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(12,'Ramesh',NULL,'Yadav','Male',42,'Lucknow','UP','2023-01-12', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(13,'Suresh',NULL,'Naidu','Male',36,'Vizag','AP','2023-01-13', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(14,'Divya',NULL,'Rao','Female',24,'Hyderabad','Telangana','2023-01-14', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(15,'Ajay',NULL,'Kumar','Male',33,'Patna','Bihar','2023-01-15', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(16,'Sneha',NULL,'Reddy','Female',28,'Hyderabad','Telangana','2023-01-16', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(17,'Raj',NULL,'Malhotra','Male',39,'Delhi','Delhi','2023-01-17', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(18,'Kavya',NULL,'Sharma','Female',22,'Jaipur','RJ','2023-01-18', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(19,'Manoj',NULL,'Tiwari','Male',41,'Bhopal','MP','2023-01-19', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(20,'Deepa',NULL,'Nair','Female',30,'Kochi','Kerala','2023-01-20', DATEADD(DAY,-6,GETDATE()), GETDATE());


-- 4. Stores
INSERT INTO Stores
(store_id, store_name, city, state, inserted_date, lastmodified_date)
VALUES
(1,'Walmart Hyd','Hyderabad','Telangana', DATEADD(DAY,-6,GETDATE()), GETDATE()),
(2,'Walmart VJA','Vijayawada','AP', DATEADD(DAY,-6,GETDATE()), GETDATE());

-- 5. Sales Transactions (generate realistic joins)
INSERT INTO Sales_Transactions
(transaction_id, customer_id, product_id, store_id, quantity, unit_price, discount, transaction_date, payment_method, last_updated, inserted_date, lastmodified_date)
VALUES
(1001,1,101,1,2,1200,100,GETDATE(),'UPI',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE()),
(1002,2,102,2,3,60,0,GETDATE(),'Card',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE()),
(1003,3,103,1,5,30,5,GETDATE(),'Cash',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE()),
(1004,4,104,2,1,250,20,GETDATE(),'UPI',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE()),
(1005,5,105,1,4,180,10,GETDATE(),'Card',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE()),
(1006,6,106,2,2,50,0,GETDATE(),'Cash',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE()),
(1007,7,107,1,3,120,15,GETDATE(),'UPI',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE()),
(1008,8,108,2,1,200,20,GETDATE(),'Card',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE()),
(1009,9,109,1,6,20,2,GETDATE(),'Cash',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE()),
(1010,10,110,2,2,40,0,GETDATE(),'UPI',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE()),
(1011,11,111,1,1,120,10,GETDATE(),'Card',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE()),
(1012,12,112,2,2,90,5,GETDATE(),'Cash',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE()),
(1013,13,113,1,3,55,0,GETDATE(),'UPI',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE()),
(1014,14,114,2,4,45,3,GETDATE(),'Card',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE()),
(1015,15,115,1,2,80,5,GETDATE(),'Cash',GETDATE(), DATEADD(DAY,-6,GETDATE()), GETDATE());