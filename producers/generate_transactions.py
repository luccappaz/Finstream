import json
import signal
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

conf = {"bootstrap.servers": "broker:9092", "client.id": "finstream-producer"}
producer = Producer(conf)

running = True


def handle_shutdown_signal(signum, frame):
    """Callback disparado quando o Docker ou o utilizador pedem para parar."""
    global running
    print(f"\n🛑 Sinal de paragem ({signum}) recebido! A encerrar o loop...")
    running = False


signal.signal(signal.SIGINT, handle_shutdown_signal)
signal.signal(signal.SIGTERM, handle_shutdown_signal)


def delivery_report(err, _):
    if err is not None:
        print(f"Failed to receive message: {err}")


def main():
    print("Initiating the transactions producer (Confluent Kafka)...")
    print("Press Ctrl + c to stop the execution.\n")

    try:
        while running:
            ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            amount = round(random.uniform(10.0, 15000.0), 2)
            transaction = {
                "transaction_id": f"TX-{random.randint(100000, 999999)}",
                "source_account": f"ACC_{random.randint(1, 150):03d}",
                "amount": amount,
                "ts": ts_str,
            }
            transaction_bytes = json.dumps(transaction).encode("utf-8")

            producer.produce(
                topic="transactions_raw",
                value=transaction_bytes,
                callback=delivery_report,
            )
            producer.poll(0)

            if amount > 10000:
                print(f"⚠️ HIGH AMOUNT ALERT: {transaction}")
            else:
                print(f"✅ Normal transaction: {transaction}")

            time.sleep(0.5)

    finally:
        print("Awaiting for the last messages to be received...")
        producer.flush()


if __name__ == "__main__":
    main()
