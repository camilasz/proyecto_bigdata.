"""
Generador de lecturas simuladas hacia Kafka.
Requiere: pip install kafka-python
Ejecutar después de iniciar Kafka y crear el topic sensor_data:
    python3 codigo/productor_kafka.py
"""
from kafka import KafkaProducer
import json, time, random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

try:
    for i in range(100):
        lectura = {
            "id": i,
            "sensor": random.choice(["Temperatura", "Humedad", "Presion"]),
            "valor": round(random.uniform(20, 32), 2),
            "unidad": "simulada",
            "tiempo": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ubicacion": f"Zona-{random.randint(1, 4)}"
        }
        producer.send("sensor_data", lectura)
        producer.flush()
        print("Enviado:", lectura)
        time.sleep(1)
finally:
    producer.close()
