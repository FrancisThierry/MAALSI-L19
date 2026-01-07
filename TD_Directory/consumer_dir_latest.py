from kafka import KafkaConsumer
import json

TOPIC_NAME = 'browse-directory'

# Configuration du Consommateur
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    group_id='sms-alert-group_latest',
    auto_offset_reset='latest',
    enable_auto_commit=False, # On gère le commit à la main
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"--- Consommateur prêt et écoute sur le topic '{TOPIC_NAME}' ---")

try:
    for message in consumer:    
        file_metadata = message.value
        print(f"Message reçu : {file_metadata['name']}")

        # Seuil de 50 Mo en octets
        SIZE_THRESHOLD = 50 * 1024 * 1024

        # Vérification de la taille
        if 'size_bytes' in file_metadata:
            if file_metadata['size_bytes'] > SIZE_THRESHOLD:
                print(f"⚠️  ALERTE SMS : Le fichier {file_metadata['name']} est trop gros ({file_metadata['size_bytes']} octets) !")
        
        # Validation manuelle du message traité
        consumer.commit()
        
except KeyboardInterrupt:
    print("\nArrêt du consommateur...")
finally:
    consumer.close()