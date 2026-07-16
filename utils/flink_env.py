from pyflink.table import EnvironmentSettings, TableEnvironment
from pathlib import Path

try:
    root_dir = Path(__file__).resolve().parent.parent
except NameError:
    root_dir = Path.cwd()


def get_paimon_t_env() -> TableEnvironment:
    jar_path = str(root_dir / "jars")
    jars = [
        f"file://{jar_path}/flink-sql-connector-kafka-5.0.0-2.2.jar",
        f"file://{jar_path}/paimon-flink-2.2-1.4.2.jar",
        f"file://{jar_path}/paimon-s3-1.4.2.jar",
        f"file://{jar_path}/hadoop-client-api-3.3.6.jar",
        f"file://{jar_path}/hadoop-client-runtime-3.3.6.jar",
    ]

    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t_env.get_config().set("pipeline.jars", ";".join(jars))

    t_env.execute_sql("""
        CREATE CATALOG IF NOT EXISTS paimon_catalog WITH (
            'type' = 'paimon',
            'warehouse' = 's3://warehouse/paimon',
            's3.endpoint' = 'http://localhost:9000', 
            's3.path-style-access' = 'true', 
            's3.access-key' = 'admin',
            's3.secret-key' = 'password',
            's3.region' = 'us-east-1'
        )
    """)
    t_env.execute_sql("USE CATALOG paimon_catalog")
    return t_env
