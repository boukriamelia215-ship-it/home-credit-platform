# Home Credit - Data Platform Bancaire

Réalisée par **Amelia Boukri** et **Melissa Belkessam** — Mastère Data Engineering EFREI 2026.

## Problématiques métier
- **Marketing** : Quels clients sans historique bancaire sont susceptibles de souscrire un premier crédit ?
- **Risque** : Peut-on prédire le risque de défaut de paiement d'un client ?
- **BI** : Quelle est la santé globale du portefeuille de crédit par région et type de crédit ?

## Architecture Médaillon Bronze / Silver / Gold

```
PostgreSQL (application_train) ──┐
                                  ├──→ feeder.py → HDFS Bronze (parquet partitionné year/month/day)
CSV (6 fichiers Kaggle) ─────────┘
                                         ↓
                                   processor.py → HDFS Silver (silver_credits parquet + Hive saveAsTable)
                                         ↓
                                   datamart.py → MySQL Gold (dm_marketing, dm_risque, dm_bi)
                                         ↓
                                   export_gold.py → HDFS Gold (parquet)
                                         ↓
                              API Flask JWT + Dashboard Streamlit
```

## Stack Technique
- **Hadoop HDFS** — Stockage distribué Bronze/Silver/Gold
- **Apache Spark 3.0** — Traitement distribué
- **Hive 2.3.2** — Requêtes SQL sur HDFS
- **PostgreSQL** — Source BDD (application_train)
- **MySQL 8.0** — Datamarts Gold
- **Flask + JWT** — API REST sécurisée
- **Streamlit** — Dashboard visualisation
- **Docker** — Containerisation (17 containers)

## Dataset
- **Source** : Kaggle — Home Credit Default Risk
- **Volume** : 307 511 clients
- **Fichiers** : application_train, bureau, bureau_balance, credit_card_balance, installments_payments, POS_CASH_balance, previous_application

## Structure du projet

```
home-credit-platform/
├── pipeline/
│   ├── feeder.py          # Ingestion PostgreSQL + CSV → HDFS Bronze
│   ├── processor.py       # Transformation Bronze → Silver + Hive
│   ├── datamart.py        # Création datamarts → MySQL Gold
│   ├── export_gold.py     # Export MySQL → HDFS Gold
│   ├── load_postgres.py   # Chargement CSV → PostgreSQL source
│   └── load_mysql.py      # Chargement CSV → MySQL
├── api/
│   └── api.py             # API Flask JWT
├── dashboard/
│   └── dashboard.py       # Dashboard Streamlit
├── sql/
│   ├── init_mysql.sql
│   └── init_postgres.sql
├── source/                # Fichiers CSV Kaggle
├── jars/                  # Connecteurs JDBC
├── docker-compose.yml
├── hadoop.env
└── hadoop-hive.env
```

## Prérequis
- Docker Desktop
- Git
- Postman
- 16 GB RAM minimum recommandé

## Installation et lancement

### 1. Cloner le repo
```bash
git clone https://github.com/boukriamelia215-ship-it/home-credit-platform.git
cd home-credit-platform
```

### 2. Télécharger les données Kaggle
Télécharger les fichiers CSV depuis Kaggle Home Credit et les placer dans le dossier `source/`

### 3. Lancer les containers Docker
```bash
docker-compose up -d
```

### 4. Charger les données dans PostgreSQL
```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/jars/postgresql-42.2.18.jar --deploy-mode client /opt/pipeline/load_postgres.py
```

### 5. Lancer le Feeder (Bronze)
```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/jars/postgresql-42.2.18.jar --deploy-mode client --executor-cores 2 --total-executor-cores 4 /opt/pipeline/feeder.py --source-dir /source --output-dir hdfs://namenode:9000/data/bronze --pg-url jdbc:postgresql://hive-metastore-postgresql:5432/metastore --pg-user hive --pg-pass hive --log-dir /opt/logs
```

### 6. Lancer le Processor (Silver)
```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 --deploy-mode client --executor-cores 2 --total-executor-cores 4 /opt/pipeline/processor.py --input-dir hdfs://namenode:9000/data/bronze --output-dir hdfs://namenode:9000/data/silver --log-dir /opt/logs
```

### 7. Lancer le Datamart (MySQL Gold)
```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --executor-cores 2 --total-executor-cores 4 --jars /opt/jars/mysql-connector-java-8.0.28.jar /opt/pipeline/datamart.py --input-dir hdfs://namenode:9000/data/silver --mysql-url jdbc:mysql://mysql:3306/datamarts --mysql-user admin --mysql-pass admin2024 --log-dir /opt/logs
```

### 8. Exporter vers HDFS Gold
```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/jars/mysql-connector-java-8.0.28.jar --deploy-mode client /opt/pipeline/export_gold.py
```

## Interfaces Web

| Interface | URL |
|-----------|-----|
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Spark Worker 1 | http://localhost:8081 |
| Spark Worker 2 | http://localhost:8082 |
| Spark UI live | http://localhost:4040 |
| API Flask | http://localhost:5000 |
| Dashboard Streamlit | http://localhost:8501 |
| Adminer | http://localhost:8086 |

## API Flask JWT

### Login
```
POST http://localhost:5000/login
Body: {"username": "admin", "password": "admin2024"}
```

### Health Check
```
GET http://localhost:5000/health
```

### Endpoints sécurisés (Bearer Token requis)
```
GET http://localhost:5000/api/marketing?page=1&size=100
GET http://localhost:5000/api/risque?page=1&size=100
GET http://localhost:5000/api/bi?page=1&size=100
```
Sans token → 401 Unauthorized

## Connexion Adminer

**PostgreSQL (source)** : Système=PostgreSQL, Serveur=hive-metastore-postgresql, User=hive, Pass=hive, DB=metastore

**MySQL (datamarts)** : Système=MySQL, Serveur=mysql, User=admin, Pass=admin2024, DB=datamarts

## Processor — Règles de validation
1. SK_ID_CURR not null
2. Suppression des doublons
3. AMT_CREDIT > 0
4. TARGET in [0, 1]
5. AMT_INCOME_TOTAL > 0

## Datamarts MySQL

| Datamart | Lignes | Description |
|----------|--------|-------------|
| dm_marketing | 44 020 | Clients sans historique bancaire |
| dm_risque | 307 511 | Tous les clients avec indicateurs de risque |
| dm_bi | 160 | Agrégations par région et type de contrat |

## Logs
Les logs sont exportés dans le dossier `logs/` :
- `feeder.txt` — logs ingestion
- `processor.txt` — logs transformation
- `datamart.txt` — logs création datamarts

## Conformité cahier des charges
- 2 sources dont 1 BDD (PostgreSQL + CSV)
- feeder.py → HDFS Bronze parquet partitionné year/month/day
- processor.py → Silver + Hive saveAsTable
- 5 règles de validation
- 6 jointures
- Agrégations
- Window function (rank)
- cache() et persist()
- datamart.py → 3 datamarts MySQL
- API REST Flask + JWT + pagination
- Dashboard Streamlit 6 graphiques + KPIs
- Logs .txt
- Aucun chemin codé en dur (argparse)
- spark-submit paramétrable
- Bronze en parquet
- saveAsTable tables internes Hive