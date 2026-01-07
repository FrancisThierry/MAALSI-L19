### TD : Simulation de Validation de Virements Bancaires

**Objectif :** Utiliser un système de messagerie (type Kafka) pour valider des transactions bancaires avant l'envoi d'une notification.

#### 1. Le Producteur (Émetteur de virement)

Le producteur doit envoyer des messages contenant les détails d'un virement vers un topic `virements-en-attente`.

* **Format du message :** `{ "transaction_id": "TX123", "iban": "FR76...", "montant": 150.00 }`

#### 2. Le Consommateur (Validateur d'IBAN)

Le consommateur écoute le topic et pour chaque message :

1. **Vérifie la validité de l'IBAN** (format, longueur, caractères).
2. **Si l'IBAN est OK :** Affiche `"SMS envoyé : Votre virement de [montant] vers [IBAN] a été validé."`
3. **Si l'IBAN est KO :** Affiche `"ERREUR : Le virement [ID] a été rejeté (IBAN invalide)."`

---

### Données de test (IBAN)


**Liste d'IBAN valides (OK) :**

* `FR76 3000 6000 0112 3456 7890 123`
* `FR76 1234 5678 9012 3456 7890 182`
* `BE68 5390 0754 3210`

**Liste d'IBAN invalides (KO) :**

* `FR76 0000 0000 0000 0000` (Trop court pour la France)
* `FR76 ABC D EF G HIJK LMNO PQR S` (Contient des caractères alphabétiques interdits ou mal placés)
* `DE98 1234 5678` (Format pays incomplet)
