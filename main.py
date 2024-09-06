from fastapi import FastAPI
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from faker import Faker
from dotenv import load_dotenv
import os
import json
import random
import numpy as np

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

TOPIC_NAME = 'transactions_cristian_2' # Nombre del topic de Kafka

# Configuración del cliente de administración de Kafka para la creación de topics
admin_client = AdminClient({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISMS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
})

# Configuración del productor de Kafka con Confluent Cloud utilizando variables de entorno
producer_conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISMS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
}

producer = Producer(producer_conf)

# Inicializar FastAPI y Faker
app = FastAPI()
fake = Faker()


# Función para verificar si un topic existe y crear uno si no existe
def create_kafka_topic(topic_name, num_partitions=3, replication_factor=3):
    topic_metadata = admin_client.list_topics(timeout=10)
    if topic_name not in topic_metadata.topics:
        print(f"Creando el topic: {topic_name}")
        new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        admin_client.create_topics([new_topic])
        print(f"Topic '{topic_name}' creado con éxito.")
    else:
        print(f"El topic '{topic_name}' ya existe.")


# Función para generar datos falsos de transacciones fraudulentas
def generate_fraudulent_data():
    transaction = {
        "time": random.uniform(0, 172792),
        "amount": random.uniform(500, 5000),
        "V1": np.random.normal(-5, 2),
        "V2": np.random.normal(5, 2),
        "V3": np.random.normal(-5, 2),
        "V4": np.random.normal(5, 2),
        "V5": np.random.normal(-5, 2),
        "V6": np.random.normal(5, 2),
        "V7": np.random.normal(-5, 2),
        "V8": np.random.normal(5, 2),
        "V9": np.random.normal(-5, 2),
        "V10": np.random.normal(5, 2),
        "V11": np.random.normal(-5, 2),
        "V12": np.random.normal(5, 2),
        "V13": np.random.normal(-5, 2),
        "V14": np.random.normal(5, 2),
        "V15": np.random.normal(-5, 2),
        "V16": np.random.normal(5, 2),
        "V17": np.random.normal(-5, 2),
        "V18": np.random.normal(5, 2),
        "V19": np.random.normal(-5, 2),
        "V20": np.random.normal(5, 2),
        "V21": np.random.normal(-5, 2),
        "V22": np.random.normal(5, 2),
        "V23": np.random.normal(-5, 2),
        "V24": np.random.normal(5, 2),
        "V25": np.random.normal(-5, 2),
        "V26": np.random.normal(5, 2),
        "V27": np.random.normal(-5, 2),
        "V28": np.random.normal(5, 2),
    }
    return transaction

# Función para enviar transacción a Kafka
def send_to_kafka(transaction, topic_name=TOPIC_NAME):
    producer.produce(topic_name, key=str(transaction["time"]), value=json.dumps(transaction))
    producer.flush()


# Endpoint que genera y produce una transacción cuando se hace una petición
@app.get("/produce-transaction")
async def produce_transaction():
    try:
        # Crear el topic si no existe
        create_kafka_topic(TOPIC_NAME)

        # Generar y enviar la transacción
        transaction = generate_fraudulent_data()
        send_to_kafka(transaction)
        return {"status": "Mensaje enviado a Kafka", "transaction": transaction}
    except Exception as e:
        return {"error": str(e)}
