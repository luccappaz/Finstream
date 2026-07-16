from pathlib import Path
import os
import sys

try:
    root_dir = Path(__file__).resolve().parent.parent
except NameError:
    root_dir = Path.cwd()

if root_dir not in sys.path:
    sys.path.append(str(root_dir))

from utils.flink_env import get_paimon_t_env


def main():
    # Define a variável de ambiente antes de qualquer coisa do Flink
    os.environ["LOG4J_PROPERTIES"] = os.path.abspath("log4j.properties")
    t_env = get_paimon_t_env()
    print("\n--- Histórico de Snapshots (Últimos 5) ---")
    snapshots = t_env.execute_sql(
        "SELECT snapshot_id, commit_kind, commit_user, commit_time FROM processed_transactions$snapshots ORDER BY snapshot_id DESC LIMIT 5"
    )
    for row in snapshots.collect():
        print(row)

    # 4. Inspecionar: Amostra dos Dados
    print("\n--- Amostra dos Dados (Primeiras 10 linhas) ---")
    data = t_env.execute_sql("SELECT * FROM processed_transactions LIMIT 10")
    for row in data.collect():
        print(row)


if __name__ == "__main__":
    main()
