# Postgres CDC to Kafka

Questo progetto utilizza TypeScript per testare le funzionalità di Change Data Capture (CDC) su PostgreSQL. Una volta avviato, il progetto creerà una tabella `students` in PostgreSQL e configurerà una publication e uno slot per la logical replication. Ogni volta che i dati nella tabella cambiano, l'evento viene catturato, trasformato e inviato a un topic Kafka.

## Pre-requisiti

- Docker & Docker-Compose
- Node.js e TypeScript
- ts-node

## Avvio del progetto

### 1. Avvio dei servizi con Docker

Naviga nella cartella `docker` e avvia i servizi utilizzando `docker-compose`:

```bash
cd docker
docker-compose up -d
```

### 2. Installazione ed Esecuzione del codice TypeScript
Dalla root del progetto esegui:

```bash
yarn install
```

Dalla cartella src, esegui:

```bash
ts-node main.tsx
```
Ciò avvierà il codice TypeScript che gestirà la configurazione di PostgreSQL e l'invio degli eventi CDC a Kafka.

### 3. Inserimento di dati in PostgreSQL
Per catturare un evento, è necessario inserire un dato nella tabella students di PostgreSQL:

- Accedi all'immagine Docker di PostgreSQL:
```bash
docker exec -it [NOME_DEL_CONTAINER_POSTGRES] bash
```
- Utilizza psql per connetterti a PostgreSQL con le credenziali specificate nel file .env:

```bash
psql -h localhost -U [UTENTE] -d [NOME_DATABASE]
```

- Una volta connesso, inserisci un record nella tabella STUDENTS:
```sql
INSERT INTO students(id, firstname, lastname, dateofbirth) VALUES (1, 'Mario', 'Rossi', '1990-01-01');
```
Ricorda di sostituire [UTENTE], [NOME_DATABASE] con i valori appropriati presenti nel tuo file .env e [NOME_DEL_CONTAINER_POSTGRES] con il nome del container.

### 4. Consumare messaggi da Kafka
Per consumare messaggi dal topic Kafka specificato:

- Accedi all'immagine Docker di Kafka:

```bash
docker exec -it [NOME_DEL_CONTAINER_POSTGRES] bash
```

- Esegui il seguente comando:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic [NOME_DEL_TUO_TOPIC] --from-beginning
```
Assicurati di sostituire [NOME_DEL_TUO_TOPIC] con il nome effettivo del tuo topic Kafka e [NOME_DEL_CONTAINER_POSTGRES] con il nome del container.