from pyflink.common import Configuration
from pathlib import Path
import sys

try:
    root_dir = Path(__file__).resolve().parent.parent
except NameError:
    root_dir = Path.cwd()

if root_dir not in sys.path:
    sys.path.append(str(root_dir))

from utils.flink_env import get_paimon_t_env


def main():
    print("Starting PyFlink fraud detection job...")

    # 2. Configura o ambiente com os JARs
    t_env = get_paimon_t_env()

    config = Configuration()
    config.set_string("execution.checkpointing.interval", "60s")
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    config.set_string("execution.checkpointing.min-pause", "5s")
    config.set_string("execution.checkpointing.timeout", "600s")
    config.set_string("restart-strategy.type", "none")
    t_env.get_config().add_configuration(config)

    t_env.execute_sql("""
        CREATE TEMPORARY TABLE transactions_raw (
            transaction_id STRING,
            source_account STRING,
            amount DOUBLE,
            ts TIMESTAMP(3),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'transactions_raw',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'fraud-detection-group',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)

    t_env.execute_sql("""
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
        )
    """)

    print("✅ Catalogo e Tabelas registados com sucesso!")
    print("⏳ Submitting continuous query to Flink Cluster...")

    pipeline = t_env.execute_sql("""
        INSERT INTO processed_transactions
        SELECT 
            transaction_id,
            source_account,
            amount,
            CASE 
                WHEN amount > 10000.0 THEN 'FRAUD_SUSPECT' 
                ELSE 'APPROVED' 
            END AS status,
            ts
        FROM transactions_raw
    """)
    try:
        pipeline.wait()
    except Exception as e:
        print("🔥 Job falhou:")
        print(e)


if __name__ == "__main__":
    main()
