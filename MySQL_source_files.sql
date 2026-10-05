-- =====================================
-- 🔷 CREATE TABLES
-- =====================================
CREATE TABLE Campaigns (
    campaign_id INT PRIMARY KEY,
    campaign_name VARCHAR(150),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(12,2),
    inserted_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    lastmodified_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE Customer_Segments (
    segment_id INT PRIMARY KEY,
    segment_name VARCHAR(100),
    inserted_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    lastmodified_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE Promotions (
    promotion_id INT PRIMARY KEY,
    product_id INT,
    campaign_id INT,
    discount_percentage DECIMAL(5,2),
    start_date DATE,
    end_date DATE,
    inserted_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    lastmodified_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (campaign_id) REFERENCES Campaigns(campaign_id)
);

CREATE TABLE Customer_Campaign_Map (
    customer_id INT,
    campaign_id INT,
    segment_id INT,
    response VARCHAR(50),
    response_date DATE,
    inserted_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    lastmodified_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (campaign_id) REFERENCES Campaigns(campaign_id),
    FOREIGN KEY (segment_id) REFERENCES Customer_Segments(segment_id)
);


-- Campaigns
INSERT INTO Campaigns 
(campaign_id, campaign_name, start_date, end_date, budget, inserted_date, lastmodified_date)
VALUES
(201,'New Year Sale','2024-01-01','2024-01-15',50000, DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(202,'Festival Offer','2024-01-10','2024-01-25',75000, DATE_SUB(NOW(), INTERVAL 6 DAY), NOW());

-- Segments
INSERT INTO Customer_Segments 
(segment_id, segment_name, inserted_date, lastmodified_date)
VALUES
(1,'High Value', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(2,'Regular', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(3,'New', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW());

-- Promotions
INSERT INTO Promotions 
(promotion_id, product_id, campaign_id, discount_percentage, start_date, end_date, inserted_date, lastmodified_date)
VALUES
(301,101,201,10,'2024-01-01','2024-01-15', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(302,102,201,5,'2024-01-01','2024-01-15', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(303,103,202,8,'2024-01-10','2024-01-25', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(304,104,202,12,'2024-01-10','2024-01-25', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(305,105,202,7,'2024-01-10','2024-01-25', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(306,106,201,6,'2024-01-01','2024-01-15', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(307,107,202,9,'2024-01-10','2024-01-25', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW());

-- Customer Campaign Map
INSERT INTO Customer_Campaign_Map 
(customer_id, campaign_id, segment_id, response, response_date, inserted_date, lastmodified_date)
VALUES
(1,201,1,'Purchased','2024-01-10', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(2,201,2,'Clicked','2024-01-11', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(3,202,3,'Viewed','2024-01-12', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(4,202,2,'Purchased','2024-01-13', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(5,201,1,'Clicked','2024-01-14', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(6,202,2,'Purchased','2024-01-15', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(7,201,3,'Viewed','2024-01-16', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(8,202,1,'Clicked','2024-01-17', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(9,201,2,'Purchased','2024-01-18', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW()),
(10,202,3,'Viewed','2024-01-19', DATE_SUB(NOW(), INTERVAL 6 DAY), NOW());

select * from Customer_Campaign_Map;