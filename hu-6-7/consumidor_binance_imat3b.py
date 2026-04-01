# -*- coding: utf-8 -*-

import json
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "34.175.118.163:9092" #"51.49.235.244:9092"
USERNAME = "kafka_client"
PASSWORD = "88b8a35dca1a04da57dc5f3e"
TOPIC = "imat3b-AVAX-VWAP"
GROUP_ID = "imat3b_group1"

def main() -> None:
    print(f"iniciando consumidor kafka sintonizando: {TOPIC}...")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )

    print("esperando velas en tiempo real... (tarda 1 minuto por vela)")
    print("pulsa ctrl+c para salir.\n")

    try:
        for consumer_record in consumer:
            print("nueva vela recibida desde kafka")
            print(f"key: {consumer_record.key}")
            print(f"value: {json.dumps(consumer_record.value, indent=2)}")
            print(f"offset: {consumer_record.offset}")
            print(f"timestamp: {consumer_record.timestamp} ms\n")
            
    except KeyboardInterrupt:
        print("\ndeteniendo consumidor...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
