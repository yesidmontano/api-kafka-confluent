import psycopg2
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import numpy as np
import pandas as pd
from sklearn.preprocessing import RobustScaler
from confluent_kafka import Consumer, KafkaException
import joblib
import json
import os
from dotenv import load_dotenv
import asyncio
import logging
from tensorflow.keras.models import load_model
from tensorflow.keras.optimizers.legacy import Adam

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

# Inicializa la aplicación FastAPI
app = FastAPI()

# Conexión a la base de datos PostgreSQL en AWS
try:
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        database=os.getenv('POSTGRES_DB'),
        port=5432,
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    cursor = conn.cursor()
    logger.info("Conectado a la base de datos PostgreSQL.")
except Exception as e:
    logger.error(f"Error al conectar a la base de datos PostgreSQL: {str(e)}")

# Crear tabla si no existe en PostgreSQL
try:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id SERIAL PRIMARY KEY,
        transaction_json JSONB,
        logistic_regression_fraud REAL,
        logistic_regression_non_fraud REAL,
        kneighbors_fraud REAL,
        kneighbors_non_fraud REAL,
        svc_fraud REAL,
        svc_non_fraud REAL,
        decision_tree_fraud REAL,
        decision_tree_non_fraud REAL,
        keras_fraud REAL,
        keras_non_fraud REAL
    )
    """)
    conn.commit()
    logger.info("Tabla 'transactions' creada o verificada.")
except Exception as e:
    logger.error(f"Error al crear la tabla: {str(e)}")

# Cargar los modelos entrenados
try:
    log_reg = joblib.load('/app/model/logistic_regression_model.pkl')
    knears_neighbors = joblib.load('/app/model/knears_neighbors_model.pkl')
    svc = joblib.load('/app/model/svc_model.pkl')
    tree_clf = joblib.load('/app/model/decision_tree_model.pkl')
    keras_model = load_model('/app/model/undersample_model.h5')
    keras_model.compile(optimizer=Adam(), loss='categorical_crossentropy', metrics=['accuracy'])
    logger.info("Modelos cargados exitosamente.")
except Exception as e:
    logger.error(f"Error al cargar los modelos: {str(e)}")

# Inicializar escaladores
scaler_amount = RobustScaler()
scaler_time = RobustScaler()

# Configuración del consumidor de Kafka
consumer_conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISMS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'group.id': 'transactions-group-1',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['transactions_cristian_2'])

# Función para consumir el mensaje desde Kafka
async def consume_from_kafka():
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                await asyncio.sleep(1)
                continue
            if msg.error():
                raise KafkaException(msg.error())
            transaction_data = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Mensaje recibido de Kafka: {transaction_data}")
            await process_transaction(transaction_data)
    except Exception as e:
        logger.error(f"Error al consumir de Kafka: {str(e)}")

# Procesar la transacción y almacenar en la base de datos PostgreSQL
async def process_transaction(transaction_data):
    try:
        logger.info(f"Procesando transacción: {transaction_data}")
        
        # Convertir la entrada en un DataFrame
        input_data = pd.DataFrame([transaction_data])
        logger.info(f"DataFrame de entrada: {input_data}")

        # Escalar las columnas 'amount' y 'time'
        input_data['scaled_amount'] = scaler_amount.fit_transform(input_data[['amount']])
        input_data['scaled_time'] = scaler_time.fit_transform(input_data[['time']])
        input_data = input_data.drop(['amount', 'time'], axis=1)
        logger.info(f"DataFrame después del escalado: {input_data}")

        # Asegurar el orden correcto de las columnas
        expected_columns = ['scaled_amount', 'scaled_time', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'V10', 'V11', 'V12', 'V13', 'V14', 'V15', 'V16', 'V17', 'V18', 'V19', 'V20', 'V21', 'V22', 'V23', 'V24', 'V25', 'V26', 'V27', 'V28']
        input_data = input_data[expected_columns]
        logger.info(f"DataFrame con columnas ordenadas: {input_data}")

        # Convertir a numpy array
        input_array = input_data.values
        logger.info(f"Array de entrada: {input_array}")

        # Hacer predicciones con todos los modelos
        logistic_reg_pred = log_reg.predict_proba(input_array)
        knears_neighbors_pred = knears_neighbors.predict_proba(input_array)
        svc_pred = svc.decision_function(input_array)

        # Convertir el valor de decision_function a probabilidades aproximadas
        svc_fraud_prob = 1 / (1 + np.exp(-svc_pred))  # Aplicar la función sigmoide
        svc_non_fraud_prob = 1 - svc_fraud_prob

        tree_pred = tree_clf.predict_proba(input_array)
        keras_pred = keras_model.predict(input_array)
        
        fraud_probability = float(keras_pred[0][1])
        non_fraud_probability = float(keras_pred[0][0])

        logger.info(f"Predicciones: logistic_reg={logistic_reg_pred}, knears_neighbors={knears_neighbors_pred}, svc={svc_fraud_prob}, tree={tree_pred}, keras={keras_pred}")

        # Convertir los valores np.float64 a float de Python
        logistic_reg_pred = logistic_reg_pred.astype(float)
        knears_neighbors_pred = knears_neighbors_pred.astype(float)
        svc_fraud_prob = svc_fraud_prob.astype(float)
        svc_non_fraud_prob = svc_non_fraud_prob.astype(float)
        tree_pred = tree_pred.astype(float)

        # Guardar el JSON completo en la base de datos
        transaction_json = json.dumps(transaction_data)

        # Almacenar la transacción en la base de datos
        cursor.execute("""
            INSERT INTO transactions (
                transaction_json,
                logistic_regression_fraud, logistic_regression_non_fraud,
                kneighbors_fraud, kneighbors_non_fraud,
                svc_fraud, svc_non_fraud,
                decision_tree_fraud, decision_tree_non_fraud,
                keras_fraud, keras_non_fraud
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            transaction_json,
            float(logistic_reg_pred[0][1]), float(logistic_reg_pred[0][0]),
            float(knears_neighbors_pred[0][1]), float(knears_neighbors_pred[0][0]),
            float(svc_fraud_prob[0]), float(svc_non_fraud_prob[0]),
            float(tree_pred[0][1]), float(tree_pred[0][0]),
            fraud_probability, non_fraud_probability
        ))
        conn.commit()
        logger.info("Transacción almacenada en la base de datos.")
    except Exception as e:
        conn.rollback()  # Deshacer la transacción en caso de error
        logger.error(f"Error al procesar la transacción: {str(e)}")

# Ruta para iniciar la tarea de consumo en segundo plano
@app.get("/start-consuming")
async def start_consuming_transactions(background_tasks: BackgroundTasks):
    background_tasks.add_task(consume_from_kafka)
    return {"message": "Consumo de Kafka iniciado en segundo plano."}
