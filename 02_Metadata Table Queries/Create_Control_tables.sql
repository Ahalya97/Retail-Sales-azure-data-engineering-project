CREATE TABLE Control_Watermark (
    table_name VARCHAR(100) PRIMARY KEY,
    source_type VARCHAR(50),   -- SQL / Oracle 
    source_schema VARCHAR(100),
    source_table VARCHAR(100),
    watermark_column VARCHAR(100),  -- column used for incremental load
    last_load_value DATETIME,       -- last successful load time
    current_load_value DATETIME,    -- current pipeline run time
    is_active BIT DEFAULT 1,
    created_date DATETIME DEFAULT GETDATE(),
    updated_date DATETIME
);

INSERT INTO Control_Watermark 
(table_name, source_type, source_schema, source_table, watermark_column, last_load_value, current_load_value)
VALUES

-- SQL SERVER TABLES
('sales_transactions','SQL','dbo','Sales_Transactions','last_updated','2024-01-01 00:00:00',NULL),
('customers','SQL','dbo','Customers','signup_date','2024-01-01 00:00:00',NULL),
('products','SQL','dbo','Products','product_id','2024-01-01 00:00:00',NULL),
('stores','SQL','dbo','Stores','store_id','2024-01-01 00:00:00',NULL),
('categories','SQL','dbo','Categories','category_id','2024-01-01 00:00:00',NULL);

-- MY SQL SERVER TABLES

INSERT INTO Control_Watermark 
(table_name, source_type, source_schema, source_table, watermark_column, last_load_value, current_load_value)
VALUES
('campaigns','MySQL','HR','Campaigns','lastmodified_date','2024-01-01 00:00:00',NULL),
('customer_segments','MySQL','HR','Customer_Segments','lastmodified_date','2024-01-01 00:00:00',NULL),
('promotions','MySQL','HR','Promotions','lastmodified_date','2024-01-01 00:00:00',NULL),
('customer_campaign_map','MySQL','HR','Customer_Campaign_Map','lastmodified_date','2024-01-01 00:00:00',NULL);

select * from Control_Watermark;