import json
import os
import time
import signal
import pandas as pd
from confluent_kafka import Producer

# Configuração do Kafka Broker local
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
conf = {"bootstrap.servers": KAFKA_BROKER, "client.id": "finstream-credit-producer"}
producer = Producer(conf)

running = True


def handle_shutdown(signum, frame):
    global running
    print(f"\n🛑 Sinal ({signum}) recebido. A fechar o produtor de crédito...")
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# Dicionários de Tradução Semântica
MAP_STATUS_CONTA = {
    "A11": "< 0 DM (Saldo Negativo)",
    "A12": "0 a 200 DM (Saldo Baixo)",
    "A13": ">= 200 DM (Saldo Alto)",
    "A14": "Sem conta corrente",
}
MAP_HISTORICO = {
    "A30": "Sem créditos",
    "A31": "Créditos pagos neste banco",
    "A32": "Créditos pagos até agora",
    "A33": "Atrasos passados",
    "A34": "Conta crítica / Outros bancos",
}
MAP_PROPOSITO = {
    "A40": "Carro novo",
    "A41": "Carro usado",
    "A42": "Móveis/Equipamentos",
    "A43": "Rádio/Televisão",
    "A44": "Eletrodomésticos",
    "A45": "Reparos",
    "A46": "Educação",
    "A49": "Negócios",
    "A410": "Outros",
}


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Falha ao enviar aplicação: {err}")
    else:
        print(
            f"✅ Aplicação enviada com sucesso para o Kafka [Partição {msg.partition()}]"
        )


def main():
    try:
        df = pd.read_csv("german.data", sep=" ", header=None)
    except FileNotFoundError:
        print("❌ Ficheiro 'german.data' não encontrado na raiz!")
        return

    print("⚡ A iniciar o Streaming de Análises de Crédito para o Kafka...")

    for idx, (_, row) in enumerate(df.iterrows()):
        if not running:
            break

        # Monta o payload estruturado e limpo
        payload = {
            "id_cliente": int(idx),
            "status_conta_corrente": MAP_STATUS_CONTA.get(str(row[0]), row[0]),
            "duracao_credito_meses": int(row[1]),
            "historico_credito": MAP_HISTORICO.get(str(row[2]), row[2]),
            "proposito_emprestimo": MAP_PROPOSITO.get(str(row[3]), row[3]),
            "valor_credito_solicitado": int(row[4]),
            "target_real": int(
                row.iloc[-1]
            ),  # Guardamos o real (1=Bom, 2=Mau) para validação posterior
        }
        print(payload)

        # Converte para bytes e dispara para o tópico
        message_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        producer.produce(
            topic="credit_applications_raw",
            value=message_bytes,
            callback=delivery_report,
        )
        producer.poll(0)

        # Simula uma nova aplicação de crédito a chegar a cada 1 segundo
        time.sleep(1.0)

    print("A esvaziar os buffers finais do Kafka...")
    producer.flush()
    print("🏁 Envio terminado.")


if __name__ == "__main__":
    main()
