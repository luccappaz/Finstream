CREATE CATALOG IF NOT EXISTS paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 's3://warehouse/paimon',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    's3.access-key' = 'admin',
    's3.secret-key' = 'password',
    's3.region' = 'us-east-1'
);

USE CATALOG paimon_catalog;

SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'execution.checkpointing.timeout' = '600s';

CREATE TEMPORARY TABLE transactions_raw (
    transaction_id STRING,
    source_account STRING,
    amount DOUBLE,
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions_raw',
    'properties.bootstrap.servers' = 'broker:9092',
    'properties.group.id' = 'fraud-detection-group',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE IF NOT EXISTS processed_transactions (
    transaction_id STRING,
    source_account STRING,
    amount DOUBLE,
    status STRING,
    ts TIMESTAMP(3),
    PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
    'bucket' = '2',
    'changelog-producer' = 'input',
    'file.format' = 'parquet'
);

INSERT INTO processed_transactions
SELECT
    transaction_id,
    source_account,
    amount,
    CASE WHEN amount > 10000.0 THEN 'FRAUD_SUSPECT' ELSE 'APPROVED' END AS status,
    ts
FROM transactions_raw;
