import json
from kafka import KafkaConsumer

# Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'demande-voyage'

# Dictionnaire pour stocker l'état actuel de toutes les demandes de voyage (Event Sourcing simplifié)
current_states = {}

# Initialisation du Consommateur
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    # Commence au plus vieux message si c'est la première fois ou si l'offset est perdu
    auto_offset_reset='earliest', 
    enable_auto_commit=True,
    # group_id permet à Kafka de savoir quel consommateur a déjà lu quels messages
    group_id='gestion-voyage-service',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"👂 Consommateur démarré. Écoute sur le topic '{TOPIC_NAME}'...")


for message in consumer:
    print(f"Message brut reçu : {message.value}")

