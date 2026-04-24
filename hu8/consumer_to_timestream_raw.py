import boto3
import json
import os
from kafka import KafkaConsumer
from datetime import datetime

def require_env(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Falta la variable de entorno requerida: {name}")
    return value


TU_ACCESS_KEY = require_env("AWS_ACCESS_KEY_ID")
TU_SECRET_KEY = require_env("AWS_SECRET_ACCESS_KEY")
TU_SESSION_TOKEN = require_env("AWS_SESSION_TOKEN")
KAFKA_PASSWORD = require_env("KAFKA_SASL_PASSWORD")

REGION = "eu-west-1"
DATABASE_NAME = "imat3a_crypto_rt"
TABLE_NAME = "imat3b_imat_raw"

print(f"Conectando a AWS Timestream en la region {REGION}...")
client = boto3.client(
    'timestream-write', 
    region_name=REGION,
    aws_access_key_id=TU_ACCESS_KEY,
    aws_secret_access_key=TU_SECRET_KEY,
    aws_session_token=TU_SESSION_TOKEN
)

print("Conectando al cluster de Kafka...")
consumer = KafkaConsumer(
    'imat3b-AVAX',
    bootstrap_servers=['51.49.235.244:9092'],
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="kafka_client",
    sasl_plain_password=KAFKA_PASSWORD,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def write_raw_to_timestream(data):
    try:
        # Quitamos los milisegundos del formato como vimos antes
        dt = datetime.strptime(data['@timestamp'], "%Y-%m-%dT%H:%M:%SZ")
        time_ms = str(int(dt.timestamp() * 1000))

        record = {
            'Dimensions': [{'Name': 'symbol', 'Value': data['symbol']}],
            'MeasureName': 'market_data',
            'MeasureValues': [
                {'Name': 'open', 'Value': str(data['open']), 'Type': 'DOUBLE'},
                {'Name': 'high', 'Value': str(data['high']), 'Type': 'DOUBLE'},
                {'Name': 'low', 'Value': str(data['low']), 'Type': 'DOUBLE'},
                {'Name': 'close', 'Value': str(data['close']), 'Type': 'DOUBLE'},
                {'Name': 'volume', 'Value': str(data['volume']), 'Type': 'DOUBLE'}
            ],
            'MeasureValueType': 'MULTI',
            'Time': time_ms
        }
        
        client.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME, Records=[record])
        print(f"✅ Éxito: Velas (OHLC) de {data['symbol']} guardadas en Timestream.")
        
    except KeyError as e:
        print(f"❌ Error: Falta un campo en el JSON de Kafka -> {e}")
    except Exception as e:
        print(f"❌ Error al guardar en Timestream: {e}")

print("Escuchando el topic imat3b-AVAX para enviarlo a AWS...")
for message in consumer:
    write_raw_to_timestream(message.value)