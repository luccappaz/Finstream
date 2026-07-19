-- sql/scored_to_paimon.sql

USE CATALOG paimon_catalog;

CREATE TEMPORARY TABLE IF NOT EXISTS credit_scored_kafka (
  id_cliente INT,
  target_real INT,
  predicao INT,
  justificativa STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'credit_applications_scored',
  'properties.bootstrap.servers' = 'broker:29092',
  'properties.group.id' = 'paimon-sink-group',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE IF NOT EXISTS credit_scored_paimon (
  id_cliente INT,
  target_real INT,
  predicao INT,
  justificativa STRING
) WITH (
  'connector' = 'paimon'
);

INSERT INTO credit_scored_paimon
SELECT * FROM credit_scored_kafka;
