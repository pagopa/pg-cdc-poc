# Postgres CDC to Azure Event Hub

This project uses TypeScript to test the Change Data Capture (CDC) functionalities on PostgreSQL. Once started, the project will set up a dummy table in Azure PostgreSQL Flexible Server and configure the logical replication. Whenever data in the table changes, the event is captured, transformed, and sent to an Azure EventHub.

## Prerequisites

- Terraform
- Azure CLI
- Node.js and TypeScript
- ts-node
- psql

## Deployment of Infrastructure

Navigate to the `terraform` directory and initiate, then apply the configurations:

```bash
cd terraform
terraform init
terraform apply
```

Confirm by typing `yes` when prompted.

The terraform code creates a resource group on azure called `pg-cdc-poc`.
It contains:

- A PostgreSQL server named `psqlserver-cdc-poc` already configured for logical replication, a key component essential for Change Data Capture (CDC). This server will house a database titled `pg-cdc-poc-db`.
- An Azure EventHub namespace named `eh-pg-cdc-poc`. Inside this namespace there is a specific EventHub named `eventhub-pg-cdc-poc`. This EventHub will be the recipient of the data change events coming from PostgreSQL.
  At the end, you must recover the primary connection string from the `Shared Access Policies` of the event hub `eventhub-pg-cdc-poc`. Copy and paste it as value of the `EVENT_HUB_CONNECTION_STRING` env variable in the `.env` file.

## Project startup

From the root of the project, install the required dependencies:

```bash
yarn install
```

Then, configure the `.env` file with the following variables:

- KAFKA_TOPIC=
- KAFKA_APP_ID=
- POSTGRESQL_PUBLICATION_NAMES=
- POSTGRESQL_SLOT_NAME=
- POSTGRESQL_HOST=
- POSTGRESQL_PORT=
- POSTGRESQL_USER=
- POSTGRESQL_PASSWORD=
- EVENT_HUB_CONNECTION_STRING=

Navigate to the src directory and run the TypeScript code:

```bash
ts-node main.tsx
```

This will handle the configuration of PostgreSQL on Azure and send CDC events to the EventHub.

## Insert Data into PostgreSQL:

To capture an event, insert data into the students table in PostgreSQL. You can do this by using psql.

```bash
psql -h [AZURE_PG_FQDN] -U psqladminun -d pg-cdc-poc-db
```

Once connected, insert a record:

```bash
INSERT INTO students (first_name, last_name, date_of_birth) VALUES (chr(floor(random() * (90-65+1) + 65)::int)::text || substr(md5(random()::text), 1, 5), chr(floor(random() * (90-65+1) + 65)::int)::text || substr(md5(random()::text), 1, 5), (current_date - floor(random()*365*25)::int));
```

Replace [AZURE_PG_FQDN] with the Fully Qualified Domain Name of your Azure PostgreSQL Flexible Server.

## Consume Messages from Kafka

You can now read the messages directly from the Azure portal.

- Log in to your Azure portal account.
- Navigate to the "Event Hubs" service and select the namespace "eh-pg-cdc-poc".
- Inside this namespace, click on the EventHub named "eventhub-pg-cdc-poc".
- Under monitoring or the related telemetry options, you should be able to view and analyze the incoming messages.
