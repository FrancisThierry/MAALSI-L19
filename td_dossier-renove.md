# TD : Système de Gestion des Primes Rénovation avec Apache Kafka

## 1. Contexte Métier

L'organisme "Rénov'Action" souhaite automatiser le traitement des dossiers de prime. Un dossier passe par plusieurs étapes de validation (Identité, Devis, Travaux). Pour garantir la robustesse du système, vous devez mettre en place une architecture événementielle utilisant **Kafka**.

---

## 2. Architecture des Flux

Vous devez créer **3 topics** principaux. Chaque message doit utiliser l'**ID Utilisateur** comme clé () pour garantir l'ordre de traitement des dossiers d'une même personne.

| Topic | Usage |
| --- | --- |
| `dossier-en-cours` | Réception de toutes les pièces (KYC, Devis, Facture). |
| `dossier-accepte` | Dossiers validés prêts pour la mise en paiement. |
| `dossier-refuse` | Dossiers rejetés avec motif du refus. |

---

## 3. Exercice 1 : Modélisation du Message (Schéma)

Un dossier doit obligatoirement contenir les informations de l'utilisateur.
**Question :** Proposez une structure JSON pour un événement de type `DEVIS_SUBMITTED`.

**Correction suggérée :**

```json
{
  "user_id": "USR-771",
  "user_details": { "nom": "Legrand", "email": "m.legrand@mail.com" },
  "dossier_id": "DOS-552",
  "status": "EN_COURS",
  "event_type": "DEVIS_SUBMITTED",
  "data": {
    "montant_ht": 4500,
    "artisan_rge": true,
    "url_document": "s3://prime-bucket/devis_552.pdf"
  },
  "timestamp": "2026-01-07T15:30:00Z"
}

```

---

## 4. Exercice 2 : Jeu d'essai (Tests unitaires)

Injectez les 5 scénarios suivants dans votre cluster Kafka pour tester la logique du système :

| ID Utilisateur | Nom | Scénario | Topic Cible Final |
| --- | --- | --- | --- |
| `U001` | Marc | Dossier complet : KYC OK + Devis OK + Facture OK. | `dossier-accepte` |
| `U002` | Sophie | Devis soumis mais artisan non certifié (RGE=false). | `dossier-refuse` |
| `U003` | Thomas | KYC soumis mais pièce d'identité expirée. | `dossier-refuse` |
| `U004` | Julie | KYC et Devis OK. En attente de la facture. | `dossier-en-cours` |
| `U005` | Ahmed | Facture reçue (8000€) > 10% du Devis (5000€). | `dossier-refuse` |

---

## 5. Exercice 3 : Implémentation du Consommateur

Écrivez la logique (en pseudo-code ou Python) d'un service capable de traiter les dossiers.

**Logique métier à implémenter :**

1. Écouter le topic `dossier-en-cours`.
2. Si `event_type == "KYC_SUBMITTED"` : Vérifier la validité. Si KO -> Envoyer vers `dossier-refuse`.
3. Si `event_type == "FACTURE_RECEIVED"` : Comparer avec le montant du devis stocké. Si écart > 10% -> Envoyer vers `dossier-refuse`.
4. Si toutes les étapes sont validées -> Envoyer vers `dossier-accepte`.

---

## 6. Exercice 4 : Infrastructure (Ligne de commande)

1. **Création des topics :**
```bash
kafka-topics --create --topic dossier-en-cours --bootstrap-server localhost:9092 --partitions 3
kafka-topics --create --topic dossier-accepte --bootstrap-server localhost:9092 --partitions 3
kafka-topics --create --topic dossier-refuse --bootstrap-server localhost:9092 --partitions 3

```


2. **Simulation d'un refus (U003 - KYC Expiré) :**
```bash
echo "U003:{\"user_id\":\"U003\",\"event_type\":\"KYC_FAILED\",\"reason\":\"ID_EXPIRED\"}" | \
kafka-console-producer --topic dossier-refuse --bootstrap-server localhost:9092 --property "parse.key=true" --property "key.separator=:"

```



---

## 7. Questions de réflexion

1. **Disponibilité :** Que se passe-t-il si le service de validation KYC tombe en panne pendant 1 heure ? Les messages sont-ils perdus ?
2. **Scalabilité :** Comment traiter 1 million de dossiers par jour avec cette architecture ?
3. **Sécurité :** Comment s'assurer que l'utilisateur `U002` ne peut pas lire les messages de `U001` ?

