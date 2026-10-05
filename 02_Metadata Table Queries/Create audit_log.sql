CREATE TABLE Pipeline_Audit_Log (
    audit_id INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name VARCHAR(150),
    table_name VARCHAR(150),
    source_type VARCHAR(50),
    load_type VARCHAR(50),              -- Full / Incremental
    pipeline_run_id VARCHAR(200),
    start_time DATETIME,
    end_time DATETIME,
    status VARCHAR(50),                 -- Started / Success / Failed
    rows_read INT,
    rows_written INT,
    error_message VARCHAR(MAX),
    created_date DATETIME DEFAULT GETDATE(),
    updated_date DATETIME
);
