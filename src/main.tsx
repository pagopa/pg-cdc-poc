import { disconnect } from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaOperation";
import {
  KafkaProducer,
  fromConfig,
  sendMessages,
} from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaProducer";
import dotenv from "dotenv";
import * as TE from "fp-ts/lib/TaskEither";
import { pipe } from "fp-ts/lib/function";
import { logLevel } from "kafkajs";
import { PgoutputPlugin } from "pg-logical-replication";
import {
  onDataEvent,
  subscribeToChanges,
} from "./database/postgresql/PostgresLogicalPg";
import {
  PGClient,
  connectPGClient,
  createPGClient,
  disconnectPGClient,
} from "./database/postgresql/PostgresOperation";
import { query } from "./database/postgresql/PostgresPg";
import { transform } from "./mapping/customMapper";

dotenv.config();
const CONFIG = {
  KAFKA: {
    CONNECTION_STRING: process.env.KAFKA_CONNECTION_SRING || "localhost:9092",
    TOPIC: process.env.KAFKA_TOPIC || "postgresql-topic",
    APP_ID: process.env.KAFKA_APP_ID || "postgresql-app",
  },
  POSTGRESQL: {
    CONNECTION_STRING:
      process.env.POSTGRESQL_CONNECTION_STRING ||
      "postgres://postgres:postgres@localhost:5432/cdc_test",
    PUBLICATION_NAMES: process.env.POSTGRESQL_PUBLICATION_NAMES || "students",
    SLOT_NAME: process.env.POSTGRESQL_SLOT_NAME || "slot_students",
  },
};

const QUERIES = {
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
  DROP_LOGICAL_REPLICATION_SLOT: `SELECT pg_drop_replication_slot(${CONFIG.POSTGRESQL.SLOT_NAME});`,
};

const processChanges =
  (topic: string, client: KafkaProducer) =>
  (messages: any[]): TE.TaskEither<Error, void> => {
    return pipe(
      transform(messages),
      sendMessages({ topic: topic }, client),
      TE.map(() => {
        console.log("Messages sent succesfully");
      }),
      TE.mapLeft((errors) => {
        console.log("Error during the message sending");
        return new Error(errors.map((error) => error.message).join(", "));
      })
    );
  };

const executeQuery = (client: PGClient, queryString: string) => {
  return pipe(
    query(client, queryString),
    TE.map(() => client),
    TE.mapLeft((error) => error)
  );
};

const cleanupAndExit = (clients: {
  pgClient: PGClient;
  kafkaClient: KafkaProducer;
}): TE.TaskEither<Error, void> => {
  console.log("Cleaning up resources...");
  return pipe(
    query(clients.pgClient, QUERIES.DROP_PUBLICATION),
    TE.chain(() =>
      query(clients.pgClient, QUERIES.DROP_LOGICAL_REPLICATION_SLOT)
    ),
    TE.chain(() => disconnectPGClient(clients.pgClient)),
    TE.chain(() =>
      pipe(
        clients.kafkaClient,
        TE.fromIO,
        TE.bindTo("client"),
        TE.chainFirst(({ client }) => disconnect(client))
      )
    ),
    TE.map(() => {
      console.log("Disconnected from Database and Kafka.");
      process.exit(0);
    })
  );
};

const main = (): void => {
  console.log("Starting...");
  pipe(
    createPGClient({ connectionString: CONFIG.POSTGRESQL.CONNECTION_STRING }),
    TE.chain((client) =>
      pipe(
        connectPGClient(client),
        TE.map(() => client)
      )
    ),
    TE.chain((client) => executeQuery(client, QUERIES.CREATE_TABLE)),
    TE.chain((client) => executeQuery(client, QUERIES.CREATE_PUBLICATION)),
    TE.chain((client) =>
      executeQuery(client, QUERIES.CREATE_LOGICAL_REPLICATION_SLOT)
    ),
    TE.chain((client) => {
      const plugin = new PgoutputPlugin({
        protoVersion: 1,
        publicationNames: [CONFIG.POSTGRESQL.PUBLICATION_NAMES],
      });

      const kafkaClient = fromConfig({
        clientId: CONFIG.KAFKA.APP_ID,
        brokers: [CONFIG.KAFKA.CONNECTION_STRING],
        logLevel: logLevel.ERROR,
      });
      const processChangesFn = processChanges(CONFIG.KAFKA.TOPIC, kafkaClient);

      return pipe(
        onDataEvent(client, processChangesFn),
        TE.chain(() => {
          return subscribeToChanges(
            client,
            plugin,
            CONFIG.POSTGRESQL.SLOT_NAME
          );
        }),
        TE.map(() => ({ pgClient: client, kafkaClient: kafkaClient }))
      );
    }),
    TE.fold(
      (error) => {
        console.error("An error occurred:", error.message);
        return TE.fromIO(() => process.exit(1));
      },
      (clients) => {
        console.log("Startup Operations completed successfully.");
        process.stdin.resume();
        process.on("SIGINT", async function () {
          console.log("Received SIGINT (Ctrl+C). Exiting...");
          return cleanupAndExit(clients);
        });
        return TE.fromIO(() => {
          console.log("Waiting for data...");
        });
      }
    )
  )();
};

main();
