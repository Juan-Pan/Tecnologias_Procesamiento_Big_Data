# -*- coding: utf-8 -*-
import json
from datetime import datetime
from kafka import KafkaProducer
from binance import Client
from binance import ThreadedWebsocketManager

BOOTSTRAP_SERVERS = "34.175.118.163:9092"#"51.49.235.244:9092"
USERNAME = "kafka_client"
PASSWORD = "88b8a35dca1a04da57dc5f3e"
TOPIC = "imat3b-AVAX"

SYMBOL = "AVAXUSDT"

def main() -> None:
    print(f"iniciando productor kafka para {TOPIC}...")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8")
    )

    def handle_kline(msg):
        k = msg['k']

        is_closed = k['x']

        if is_closed:
            ts_ms = int(k['t'])

            dt_utc = datetime.utcfromtimestamp(ts_ms / 1000.0)
            timestamp_str = dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

            key = SYMBOL
            value = {
                "symbol": SYMBOL,
                "@timestamp": timestamp_str,
                "close": float(k['c']),
                "volume": float(k['v'])
            }

            print(f"vela cerrada -> mensaje a enviar: {key} {str(value)}")

            producer.send(
                topic=TOPIC,
                key=key,
                value=value,
                timestamp_ms=ts_ms
            )
            producer.flush()

    print(f"conectando a binance para {SYMBOL} en velas de 1 minuto...")
    twm = ThreadedWebsocketManager()
    twm.start()

    twm.start_kline_socket(
        symbol=SYMBOL,
        interval=Client.KLINE_INTERVAL_1MINUTE,
        callback=handle_kline
    )

    input("escuchando mercado en tiempo real... pulsa enter para detener.\n")
    twm.stop()
    producer.close()

if __name__ == "__main__":
    main()
