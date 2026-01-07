import json
import time
from kafka import KafkaProducer

# Configuration du producteur
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

def envoyer_evt(user_id, type_evt, statut):
    data = {
        "id_utilisateur": user_id,
        "type_evenement": type_evt,
        "statut": statut,
        "timestamp": time.time()
    }
    
    # Envoi du message au topic 'demandes-cartes'
    producer.send('demandes-cartes', key=user_id, value=data)
    print(f"Evenement envoye : {user_id} - {type_evt} - {statut}")
    producer.flush()

if __name__ == "__main__":
    print("Demarrage de la simulation NACAIRE...")
    
    # # Scenario 1 : Dossier valide
    # print("Utilisateur 001 : Parcours succes")
    # envoyer_evt("user_001", "INITIALISATION", "EN_COURS")
    # time.sleep(1)
    # envoyer_evt("user_001", "SOCIAL", "ACCEPTE")
    # time.sleep(1)
    # envoyer_evt("user_001", "KYC", "ACCEPTE")

    # # Scenario 2 : Dossier refuse
    # print("Utilisateur 002 : Rejet KYC")
    # envoyer_evt("user_002", "SOCIAL", "ACCEPTE")
    # envoyer_evt("user_002", "KYC", "REFUSE")
    
    data = [
        {"id_utilisateur": "user_003", "type_evenement": "INITIALISATION", "statut": "EN_COURS"},
        {"id_utilisateur": "user_003", "type_evenement": "SOCIAL", "statut": "ACCEPTE"},
        {"id_utilisateur": "user_003", "type_evenement": "KYC", "statut": "ACCEPTE"},
        {"id_utilisateur": "user_004", "type_evenement": "INITIALISATION", "statut": "EN_COURS"},
        {"id_utilisateur": "user_004", "type_evenement": "SOCIAL", "statut": "REFUSE"},
        {"id_utilisateur": "user_004", "type_evenement": "KYC", "statut": "ACCEPTE"},
        {"id_utilisateur": "user_005", "type_evenement": "INITIALISATION", "statut": "EN_COURS"},
        {"id_utilisateur": "user_005", "type_evenement": "SOCIAL", "statut": "ACCEPTE"},
        {"id_utilisateur": "user_005", "type_evenement": "KYC", "statut": "REFUSE"},
        {"id_utilisateur": "user_006", "type_evenement": "INITIALISATION", "statut": "EN_COURS"},
        {"id_utilisateur": "user_006", "type_evenement": "SOCIAL", "statut": "ACCEPTE"},
        {"id_utilisateur": "user_006", "type_evenement": "KYC", "statut": "ACCEPTE"},
        {"id_utilisateur": "user_007", "type_evenement": "INITIALISATION", "statut": "EN_COURS"},
        {"id_utilisateur": "user_007", "type_evenement": "SOCIAL", "statut": "REFUSE"},
        {"id_utilisateur": "user_008", "type_evenement": "INITIALISATION", "statut": "EN_COURS"},
        {"id_utilisateur": "user_008", "type_evenement": "SOCIAL", "statut": "ACCEPTE"},
        {"id_utilisateur": "user_008", "type_evenement": "KYC", "statut": "ACCEPTE"},
        {"id_utilisateur": "user_009", "type_evenement": "INITIALISATION", "statut": "EN_COURS"},
        {"id_utilisateur": "user_009", "type_evenement": "SOCIAL", "statut": "EN_COURS"},
        {"id_utilisateur": "user_010", "type_evenement": "KYC", "statut": "REFUSE"}
    ]
    
    for evt in data:
        envoyer_evt(evt["id_utilisateur"], evt["type_evenement"], evt["statut"])
        time.sleep(1)

    # Envoi de la liste des evenements sur le topic 'evenements'
    # producer.send('evenements', value=json.dumps(data).encode('utf-8'))
    print("Evenements envoyes :")
    print(json.dumps(data, indent=4))
    producer.flush()
    
    
    
    