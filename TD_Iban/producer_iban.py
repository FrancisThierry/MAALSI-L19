from kafka import KafkaProducer
import json
import time
import random

# Configuration du Producteur
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

virements_tests = [
    {"id": "TX1", "iban": "FR7630006000011234567890123", "montant": 150.0},
    {"id": "TX2", "iban": "FR76000000000", "montant": 50.0},
    {"id": "TX3", "iban": "BE68539007543210", "montant": 1200.0},
    {"id": "TX4", "iban": "FR76ABCDE12345", "montant": 10.0},
    {"id": "TX5", "iban": "FR7612345678901234567890182", "montant": 95.0}
]

print("--- Envoi des virements en cours ---")

for virement in virements_tests:
    # Pause aléatoire pour simuler un flux réel
    time.sleep(random.uniform(0.5, 2))
    
    # Envoi au topic
    future = producer.send('virements-en-attente', key=virement["id"], value=virement)
    
    # On attend la confirmation de réception par Kafka
    record_metadata = future.get(timeout=10)
    print(f"Envoyé: {virement['id']} (Partition: {record_metadata.partition}, Offset: {record_metadata.offset})")

producer.flush()