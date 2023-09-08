import dotenv from "dotenv";
import { PgoutputPlugin } from "pg-logical-replication";

dotenv.config();

export const PGCONFIG = {
  HOST: process.env.POSTGRESQL_HOST || "localhost",
  PORT: parseInt(process.env.POSTGRESQL_PORT, 10) || 5432,
  DATABASE: process.env.POSTGRESQL_DATABASE || "pg-cdc-poc-db",
  USER: process.env.POSTGRESQL_USER || "postgres",
  PASSWORD: process.env.POSTGRESQL_PASSWORD || "postgres",
  PUBLICATION_NAMES: process.env.POSTGRESQL_PUBLICATION_NAMES || "students",
  SLOT_NAME: process.env.POSTGRESQL_SLOT_NAME || "slot_students",
};

export const EHCONFIG = {
  CONNECTION_STRING:
    process.env.EVENT_HUB_CONNECTION_STRING || "localhost:9093",
};

export const plugin = new PgoutputPlugin({
  protoVersion: 1,
  publicationNames: [PGCONFIG.PUBLICATION_NAMES],
});
