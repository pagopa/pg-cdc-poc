import { PGCONFIG } from "../config/config";

export const QUERIES = {
  CREATE_TABLE: `
        CREATE TABLE IF NOT EXISTS students (
          id SERIAL PRIMARY KEY,
          first_name VARCHAR(255) NOT NULL,
          last_name VARCHAR(255) NOT NULL,
          date_of_birth DATE NOT NULL
        );
      `,
  CREATE_PUBLICATION: `CREATE PUBLICATION ${PGCONFIG.PUBLICATION_NAMES} FOR ALL TABLES;`,
  DROP_PUBLICATION: `DROP PUBLICATION ${PGCONFIG.PUBLICATION_NAMES};`,
  CREATE_LOGICAL_REPLICATION_SLOT: `SELECT pg_create_logical_replication_slot('${PGCONFIG.SLOT_NAME}', 'pgoutput');`,
  DROP_LOGICAL_REPLICATION_SLOT: `SELECT pg_drop_replication_slot('${PGCONFIG.SLOT_NAME}');`,
};
