-- Comandos
CREATE CATALOG paimon_catalog WITH (
     'type' = 'paimon',
     'warehouse' = 's3://warehouse/paimon',
     's3.endpoint' = 'http://minio:9000',
     's3.access-key' = 'admin',
     's3.secret-key' = 'password',
     's3.path-style-access' = 'true',
     's3.region' = 'us-east-1'
);

USE CATALOG paimon_catalog;

SHOW TABLES;

DESCRIBE processed_transactions;

SET 'sql-client.execution.result-mode' = 'tableau';

SELECT * FROM processed_transactions LIMIT 10;
