import { PgoutputPlugin } from "pg-logical-replication";

export const CONFIG = {
  POSTGRESQL: {
    HOST: process.env.POSTGRESQL_HOST || "localhost",
    PORT: parseInt(process.env.POSTGRESQL_PORT, 10) || 5432,
    DATABASE: process.env.POSTGRESQL_DATABASE || "pg-cdc-poc-db",
    USER: process.env.POSTGRESQL_USER || "postgres",
    PASSWORD: process.env.POSTGRESQL_PASSWORD || "postgres",
    PUBLICATION_NAMES: process.env.POSTGRESQL_PUBLICATION_NAMES || "students",
    SLOT_NAME: process.env.POSTGRESQL_SLOT_NAME || "slot_students",
  },
  EVENTHUB: {
    CONNECTION_STRING:
      process.env.EVENT_HUB_CONNECTION_STRING || "localhost:9093",
  },
};

export const QUERIES = {
  CREATE_TABLE: `
      CREATE TABLE IF NOT EXISTS students (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        date_of_birth DATE NOT NULL
      );
    `,
  CREATE_PUBLICATION: `CREATE PUBLICATION ${CONFIG.POSTGRESQL.PUBLICATION_NAMES} FOR ALL TABLES;`,
  DROP_PUBLICATION: `DROP PUBLICATION ${CONFIG.POSTGRESQL.PUBLICATION_NAMES};`,
  CREATE_LOGICAL_REPLICATION_SLOT: `SELECT pg_create_logical_replication_slot('${CONFIG.POSTGRESQL.SLOT_NAME}', 'pgoutput');`,
  DROP_LOGICAL_REPLICATION_SLOT: `SELECT pg_drop_replication_slot('${CONFIG.POSTGRESQL.SLOT_NAME}');`,
};

export const plugin = new PgoutputPlugin({
  protoVersion: 1,
  publicationNames: [CONFIG.POSTGRESQL.PUBLICATION_NAMES],
});
