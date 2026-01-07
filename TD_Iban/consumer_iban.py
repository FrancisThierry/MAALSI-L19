from kafka import KafkaConsumer
import json

# Règles de gestion
IBAN_LENGTHS = {"FR": 27, "BE": 16, "DE": 22}

# Configuration du Consommateur
consumer = KafkaConsumer(
    'virements-en-attente',
    bootstrap_servers=['localhost:9092'],
    group_id='validation-virement-group', # Identifiant du groupe
    auto_offset_reset='earliest',         # Lit depuis le début si premier passage
    enable_auto_commit=False,              # Valide automatiquement la lecture (offset)
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def check_iban(iban):
    iban = iban.replace(" ", "")
    code_pays = iban[:2]
    if code_pays not in IBAN_LENGTHS:
        return False, "Pays non géré"
    if len(iban) != IBAN_LENGTHS[code_pays]:
        return False, f"Format {code_pays} invalide (longueur)"
    return True, "OK"

print("--- Consommateur prêt et écoute... ---")

for message in consumer:
    transaction = message.value
    v_id = transaction['id']
    v_iban = transaction['iban']
    
    is_valid, reason = check_iban(v_iban)
    
    if is_valid:
        print(f"✅ SUCCESS [Partition {message.partition}] : {v_id} validé.")
        print(f"   >>> SMS envoyé au client pour le virement vers {v_iban[:4]}...")
        message.offset_commit()  # Valider la lecture du message
    else:
        print(f"❌ REJET [Partition {message.partition}] : {v_id} - Motif: {reason}")