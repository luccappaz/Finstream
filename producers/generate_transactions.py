import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

conf = {"bootstrap.servers": "localhost:9092", "client.id": "finstream-producer"}
producer = Producer(conf)


def delivery_report(err, _):
    if err is not None:
        print(f"Failed to receive message: {err}")


def main():
    print("Initiating the transactions producer (Confluent Kafka)...")
    print("Press Ctrl + c to stop the execution.\n")

    try:
        while True:
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

    except KeyboardInterrupt:
        print("\n🛑 Execution stopped by user.")
    finally:
        print("Awaiting for the last messages to be received...")
        producer.flush()


if __name__ == "__main__":
    main()
