from kafka import KafkaProducer
import json
import time
import random

# Inicializar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

niveles_triage = ["Reanimacion", "Emergencia", "Urgencia", "Prioritario", "No Urgente"]

print("Iniciando simulación de ingreso de pacientes a urgencias...")

while True:
    # Simular los datos de ingreso de un paciente en tiempo real
    datos_ingreso = {
        'id_sede_hospital': random.randint(1, 3), # Tenemos 3 sedes hospitalarias
        'nivel_triage': random.choice(niveles_triage),
        'timestamp': time.time()
    }
    
    producer.send('flujo_urgencias', value=datos_ingreso)
    print(f"Nuevo ingreso detectado: Sede {datos_ingreso['id_sede_hospital']} - Triage: {datos_ingreso['nivel_triage']}")
    
    # Llega un paciente cada 1 a 3 segundos
    time.sleep(random.uniform(1, 3))
