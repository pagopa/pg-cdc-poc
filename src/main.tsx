import { disconnect } from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaOperation";
import {
  AzureEventhubSas,
  AzureEventhubSasFromString,
  KafkaProducerCompact,
  fromSas,
  sendMessages as sendMessagesEH,
} from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaProducerCompact";
import * as E from "fp-ts/Either";

import dotenv from "dotenv";
import * as TE from "fp-ts/lib/TaskEither";
import { pipe } from "fp-ts/lib/function";
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
import { query } from "./database/postgresql/PostgresPG";
import { transform } from "./mapping/customMapper";
import { Student } from "./model/student";

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
    HOST: process.env.POSTGRESQL_HOST || "localhost",
    PORT: parseInt(process.env.POSTGRESQL_PORT!) || 5432,
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
  DROP_LOGICAL_REPLICATION_SLOT: `SELECT pg_drop_replication_slot('${CONFIG.POSTGRESQL.SLOT_NAME}');`,
};

let clientsToClean: {
  pgClient: PGClient;
  kafkaClient: KafkaProducerCompact<Student>;
};

// TO USE WHEN WORKING WITH LOCAL DOCKERIZED KAFKA
// const processChanges =
//   (topic: string, client: KafkaProducer) =>
//   (messages: any[]): TE.TaskEither<Error, void> => {
//     return pipe(
//       transform(messages),
//       sendMessages({ topic: topic }, client),
//       TE.map(() => {
//         console.log("Messages sent succesfully");
//       }),
//       TE.mapLeft((errors) => {
//         console.log("Error during the message sending");
//         return new Error(errors.map((error) => error.message).join(", "));
//       })
//     );
//   };

const executeQuery = (client: PGClient, queryString: string) => {
  return pipe(
    query(client, queryString),
    TE.map(() => client),
    TE.mapLeft((error) => error)
  );
};

const processChangesEH =
  (client: KafkaProducerCompact<Student>) =>
  (messages: any[]): TE.TaskEither<Error, void> => {
    return pipe(
      transform(messages),
      sendMessagesEH(client),
      TE.map(() => {
        console.log("Messages sent succesfully");
      }),
      TE.mapLeft((errors) => {
        console.log("Error during the message sending");
        return new Error(errors.map((error) => error.message).join(", "));
      })
    );
  };

const cleanupAndExit = (clients: {
  pgClient: PGClient;
  kafkaClient: KafkaProducerCompact<Student>;
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
        TE.chainFirst(({ client }) => disconnect(client.producer))
      )
    ),
    TE.map(() => {
      console.log("Disconnected from Database and Kafka.");
      process.exit(0);
    }),
    TE.mapLeft((error) => {
      console.log("Error during the exit ", error);
      process.exit(1);
    })
  );
};

const main = (): void => {
  console.log("Starting...");
  pipe(
    TE.fromEither(
      pipe(
        AzureEventhubSasFromString.decode(CONFIG.EVENTHUB.CONNECTION_STRING),
        E.fold(
          (errors) => {
            console.log("Error during decoding Event Hub SAS", errors);
            return E.left(new Error("Decoding failed"));
          },
          (sas) => E.right(sas)
        ),
        E.map((sas) => {
          console.log("Event Hub SAS decoded");
          return sas;
        })
      )
    ) as TE.TaskEither<never, AzureEventhubSas>,
    TE.chain((sas: AzureEventhubSas) => {
      return pipe(
        createPGClient({
          host: CONFIG.POSTGRESQL.HOST,
          port: CONFIG.POSTGRESQL.PORT,
          database: CONFIG.POSTGRESQL.DATABASE,
          user: CONFIG.POSTGRESQL.USER,
          password: CONFIG.POSTGRESQL.PASSWORD,
          ssl: {
            rejectUnauthorized: false,
          },
        }),
        TE.chain((client) => {
          const kafkaClient = fromSas(sas, undefined);
          clientsToClean = {
            pgClient: client,
            kafkaClient: kafkaClient,
          };
          return pipe(
            connectPGClient(client),
            TE.map(() => client)
          );
        }),
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

          return pipe(
            onDataEvent(client, processChangesEH(clientsToClean.kafkaClient)),
            TE.chain(() => {
              return subscribeToChanges(
                client,
                plugin,
                CONFIG.POSTGRESQL.SLOT_NAME
              );
            }),
            TE.map(() => {
              return {
                pgClient: client,
                kafkaClient: clientsToClean.kafkaClient,
              };
            })
          );
        }),
        TE.fold(
          (error) => {
            console.error("An error occurred:", error.message);
            cleanupAndExit(clientsToClean)();
            return TE.fromIO(() => process.exit(1));
          },
          (clients) => {
            console.log("Startup Operations completed successfully.");
            process.stdin.resume();
            process.on("SIGINT", async function () {
              console.log("Received SIGINT (Ctrl+C). Exiting...");
              return cleanupAndExit(clients)();
            });
            return TE.fromIO(() => {
              console.log("Waiting for data...");
            });
          }
        )
      );
    })
  )().then((outcome) => console.log(outcome));
};

main();
