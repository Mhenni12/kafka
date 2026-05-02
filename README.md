# TP Apache Kafka — Pipeline POS mono-machine

Projet Maven Java 17 / Kafka 3.9 (mode KRaft, sans Docker, sans ZooKeeper) accompagnant le TP
"Apache Kafka — Architecture, Déploiement et Programmation".

## Contenu du dépôt

```
tp-kafka-java/
├── pom.xml
└── src/main/java/tn/utm/kafka/
    ├── SimpleProducer.java        ← Partie 4.2
    ├── SimpleConsumer.java        ← Partie 4.3
    ├── Commande.java              ← Exercice 4.C
    ├── CommandeSerializer.java    ← Exercice 4.C bonus
    ├── CommandeDeserializer.java
    ├── CommandeProducer.java
    ├── CommandeConsumer.java
    └── projet/                    ← Mini-projet Partie 6
        ├── Evenement.java
        ├── JsonUtil.java
        ├── SimulateurCaisse.java
        ├── ChiffreAffairesParVille.java
        └── DetecteurAnomalies.java
```

## Pré-requis

- **Java 17+** (`java -version`)
- **Maven 3.8+** (`mvn -version`)
- **Apache Kafka 3.9.0** installé dans `$KAFKA_HOME` et démarré en mode KRaft sur `localhost:9092`

## Démarrage rapide

### 1. Démarrer Kafka

```bash
# Une seule fois (formatage du stockage)
KAFKA_CLUSTER_ID=$(kafka-storage.sh random-uuid)
kafka-storage.sh format --config ~/kafka-data/server.properties --cluster-id $KAFKA_CLUSTER_ID

# A chaque session
kafka-server-start.sh -daemon ~/kafka-data/server.properties
```

### 2. Créer les topics

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ventes        --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic commandes     --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic pos-events    --partitions 4 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic alertes-retours --partitions 2 --replication-factor 1
```

### 3. Compiler

```bash
cd tp-kafka-java
mvn -q clean package
```

### 4. Lancer les exemples

| But | Commande |
|---|---|
| Producteur simple | `mvn exec:java -Dexec.mainClass="tn.utm.kafka.SimpleProducer"` |
| Consommateur simple | `mvn exec:java -Dexec.mainClass="tn.utm.kafka.SimpleConsumer"` |
| Producteur Commande JSON | `mvn exec:java -Dexec.mainClass="tn.utm.kafka.CommandeProducer"` |
| Consommateur Commande JSON | `mvn exec:java -Dexec.mainClass="tn.utm.kafka.CommandeConsumer"` |
| Simulateur caisse | `mvn exec:java -Dexec.mainClass="tn.utm.kafka.projet.SimulateurCaisse" -Dexec.args="CAISSE-01"` |
| Chiffre d'affaires par ville | `mvn exec:java -Dexec.mainClass="tn.utm.kafka.projet.ChiffreAffairesParVille"` |
| Détecteur d'anomalies | `mvn exec:java -Dexec.mainClass="tn.utm.kafka.projet.DetecteurAnomalies"` |

### 5. Démonstration mini-projet (5 terminaux)

```bash
# T1
mvn exec:java -Dexec.mainClass="tn.utm.kafka.projet.SimulateurCaisse" -Dexec.args="CAISSE-01"
# T2
mvn exec:java -Dexec.mainClass="tn.utm.kafka.projet.SimulateurCaisse" -Dexec.args="CAISSE-02"
# T3
mvn exec:java -Dexec.mainClass="tn.utm.kafka.projet.ChiffreAffairesParVille"
# T4
mvn exec:java -Dexec.mainClass="tn.utm.kafka.projet.DetecteurAnomalies"
# T5 — observation des alertes
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic alertes-retours --from-beginning --property "print.key=true"
```

## Configuration Kafka utilisée

- `acks=all` + `enable.idempotence=true` côté producers → at-least-once sans doublons.
- `enable.auto.commit=false` + `commitSync()` côté consumers → at-least-once contrôlé.
- Clé du topic `pos-events` = ville → ordonnancement par ville.

## Surveillance

```bash
# LAG du groupe ChiffreAffaires
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ca-1

# LAG du detecteur d'anomalies
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group alerte-1

# Taille des partitions
kafka-log-dirs.sh --bootstrap-server localhost:9092 --describe --json | jq .
```

## Arrêt

`Ctrl+C` dans chaque terminal d'application, puis :

```bash
kafka-server-stop.sh
```

## Licence

Ce projet est fourni à titre pédagogique pour le TP Apache Kafka.
