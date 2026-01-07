import json
import os
import time
import random  # Importation pour l'aléa
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = 'localhost:9092' 
TOPIC_NAME = 'browse-directory'
REQUEST_ID = 'DIR-2025-003'

# Initialisation du Producteur
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_file_metadata(item_path, item_name):
    """Extrait les métadonnées d'un fichier."""
    stats = os.stat(item_path)
    return {
        "request_id": REQUEST_ID,
        "name": item_name,
        "path": item_path,
        "size_mb": stats.st_size / (1024 * 1024),
        "last_modified": time.ctime(stats.st_mtime),
        "extension": os.path.splitext(item_name)[1].lower(),
        "is_file": os.path.isfile(item_path)
    }

def list_recursive_kafka(path, indent=0):
    """Parcourt les répertoires avec une temporisation aléatoire."""
    try:
        items = os.listdir(path)
        
        for item in items:
            item_path = os.path.join(path, item)
            metadata = get_file_metadata(item_path, item)
            
            # --- Ajout de la temporisation aléatoire ---
            # Génère un délai entre 0.1 et 0.8 secondes
            delay = random.uniform(0.1, 0.8)
            time.sleep(delay)
            
            print(f"{'  ' * indent}|-- [{delay:.2f}s] Envoi : {item}")
            producer.send(TOPIC_NAME, value=metadata)
            
            if os.path.isdir(item_path):
                list_recursive_kafka(item_path, indent + 1)
                
    except PermissionError:
        print(f"{'  ' * indent}[!] Permission refusée : {path}")
    except FileNotFoundError:
        print(f"{'  ' * indent}[!] Répertoire introuvable.")

# --- Exécution ---
myPath = "C:/Users/thier/OneDrive/Documents"
# myPath = "C:/projets/MAALSI-L19"

print(f"Début de l'envoi avec délais aléatoires pour : {myPath}")
list_recursive_kafka(myPath)

producer.flush()
producer.close()
print("Flux terminé.")