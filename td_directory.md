
## Objectif du TD

Créer un système de monitoring qui :

1. **Producteur :** Parcourt un répertoire, liste les fichiers et envoie leurs métadonnées (nom, taille).
2. **Consommateur :** Reçoit les données, simule l'envoi d'un SMS si le fichier dépasse 50 Mo.
3. **Concepts clés :** Manipuler les **Partitions** pour le parallélisme, les **Group IDs** pour la répartition de la charge, et les **Offsets** pour la reprise après erreur.

---

## Exercice 1 : Le Producteur Récursif

Le but est d'envoyer chaque information de fichier comme un message dans un topic appelé `file-monitor`.

**Instructions :**

* Créez un script qui parcourt récursivement un dossier source.
* Pour chaque fichier trouvé, envoyez un message contenant : `{ "path": "...", "size_mb": ... }`.
* **Contrainte :** Ajoutez une pause aléatoire entre chaque dossier traité pour simuler un flux continu et observer le comportement des consommateurs en temps réel.

---

## Exercice 2 : Consommateur Simple et Group ID

### Étape A : Premier consommateur

Développez un consommateur appartenant au groupe `sms-alert-group`.

* Il doit lire les messages du topic.
* Si `size_mb > 50`, affichez : `"ALERTE SMS : Le fichier [nom] est trop gros !"`.

### Étape B : Passage à l'échelle (Scalability)

1. Lancez une **deuxième instance** du même consommateur avec le **même Group ID**.
2. **Observation :** Observez comment les partitions du topic sont réparties entre les deux instances. Un fichier traité par le Consommateur A ne doit pas être traité par le Consommateur B.
3. Lancez une troisième instance avec un **Group ID différent** (ex: `log-group`).
4. **Observation :** Notez que ce nouveau groupe reçoit *tous* les messages, indépendamment du premier groupe.

> **Note sur les Partitions :** Si votre topic n'a qu'une seule partition, un seul consommateur par groupe sera actif. Pour tester le parallélisme, assurez-vous que votre topic a au moins 2 ou 4 partitions.

---

## Exercice 3 : Gestion des Offsets et Tolérance aux Pannes

L'offset représente la position du consommateur dans la partition.

**Scénario de panne :**

1. Lancez votre producteur pour qu'il remplisse le topic.
2. Lancez votre consommateur, laissez-le traiter quelques fichiers, puis **arrêtez-le brutalement** (Ctrl+C).
3. Ajoutez de nouveaux fichiers dans vos dossiers (le producteur continue de tourner).
4. Relancez le consommateur.

**À vérifier :**

* **Comportement par défaut :** Le consommateur reprend-il là où il s'est arrêté (Offset sauvegardé) ou ignore-t-il les messages passés ?
* **Modification :** Configurez le consommateur pour qu'il recommence au tout début du topic (`auto.offset.reset: earliest`) et observez le renvoi des alertes SMS pour les fichiers déjà traités.

---

## Questions de synthèse

| Concept | Rôle dans ce TD |
| --- | --- |
| **Partition** | Permet de diviser la liste des fichiers pour les traiter plus vite. |
| **Group ID** | Permet à plusieurs instances de se partager le travail sans doublon. |
| **Offset** | Marque-page qui évite d'envoyer deux fois le même SMS pour un même fichier. |
