import { disconnect } from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaOperation";
import {
  AzureEventhubSasFromString,
  KafkaProducerCompact,
  fromSas,
  sendMessages as sendMessagesEH,
} from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaProducerCompact";
import dotenv from "dotenv";
import * as E from "fp-ts/Either";
import * as C from "fp-ts/lib/Console";
import * as IO from "fp-ts/lib/IO";
import * as TE from "fp-ts/lib/TaskEither";
import { pipe } from "fp-ts/lib/function";
import { withLogger } from "logging-ts/lib/IO";
import { PgoutputPlugin } from "pg-logical-replication";
import {
  onDataEvent,
  subscribeToChanges,
} from "./database/postgresql/PostgresLogicalPg";
import {
  PGClient,
  createPGClient,
  disconnectPGClient,
  disconnectPGLogicalClient,
} from "./database/postgresql/PostgresOperation";
import { query } from "./database/postgresql/PostgresPg";
import { transform } from "./mapping/customMapper";
import { Student } from "./model/student";

dotenv.config();

const CONFIG = {
  KAFKA: {
    CONNECTION_STRING: process.env.KAFKA_CONNECTION_SRING ?? "localhost:9092",
    TOPIC: process.env.KAFKA_TOPIC || "postgresql-topic",
    APP_ID: process.env.KAFKA_APP_ID || "postgresql-app",
  },
  POSTGRESQL: {
    CONNECTION_STRING:
      process.env.POSTGRESQL_CONNECTION_STRING ||
      "postgres://postgres:postgres@localhost:5432/cdc_test",
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

const log = withLogger(IO.io)(C.log);

const executeQuery = (client: PGClient, queryString: string) =>
  pipe(
    query(client, queryString),
    TE.map(() => client),
    TE.mapLeft((error) => error)
  );

const processChangesEH =
  (client: KafkaProducerCompact<Student>) =>
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (messages: any[]): TE.TaskEither<Error, void> =>
    pipe(
      transform(messages),
      sendMessagesEH(client),
      TE.map(() => void log(() => "Messages sent succesfully")),
      TE.mapLeft(
        (errors) =>
          new Error(
            `Error during the message sending - ${errors
              .map((error) => error.message)
              .join(", ")}`
          )
      )
    );

const cleanupAndExit = (clients: {
  pgClient: PGClient;
  kafkaClient: KafkaProducerCompact<Student>;
}): TE.TaskEither<Error, void> =>
  pipe(
    disconnectPGLogicalClient(clients.pgClient),
    TE.chain(() => query(clients.pgClient, QUERIES.DROP_PUBLICATION)),
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
      log(() => "Disconnected from Database and Kafka.");
      process.exit(0);
    }),
    TE.mapLeft((error) => {
      log(() => `Error during the exit - ${error}`);
      process.exit(1);
    })
  );

const getSas = (): TE.TaskEither<Error, KafkaProducerCompact<Student>> =>
  TE.fromEither(
    pipe(
      AzureEventhubSasFromString.decode(CONFIG.EVENTHUB.CONNECTION_STRING),
      E.fold(
        (errors) =>
          E.left(new Error(`Error during decoding Event Hub SAS - ${errors}`)),
        (sas) => E.right(fromSas(sas))
      )
    )
  );

const pgConfig = {
  host: CONFIG.POSTGRESQL.HOST,
  port: CONFIG.POSTGRESQL.PORT,
  database: CONFIG.POSTGRESQL.DATABASE,
  user: CONFIG.POSTGRESQL.USER,
  password: CONFIG.POSTGRESQL.PASSWORD,
  ssl: {
    rejectUnauthorized: false,
  },
};

const plugin = new PgoutputPlugin({
  protoVersion: 1,
  publicationNames: [CONFIG.POSTGRESQL.PUBLICATION_NAMES],
});

const main = () =>
  pipe(
    TE.Do,
    TE.bind("client", () => createPGClient(pgConfig)),
    TE.bind("messagingClient", () => getSas()),
    TE.chainFirst(({ client }) => executeQuery(client, QUERIES.CREATE_TABLE)),
    TE.chainFirst(({ client }) =>
      executeQuery(client, QUERIES.CREATE_PUBLICATION)
    ),
    TE.chainFirst(({ client }) =>
      executeQuery(client, QUERIES.CREATE_LOGICAL_REPLICATION_SLOT)
    ),
    TE.chainFirst(({ messagingClient, client }) =>
      pipe(
        onDataEvent(client, processChangesEH(messagingClient)),
        TE.chain(() =>
          subscribeToChanges(client, plugin, CONFIG.POSTGRESQL.SLOT_NAME)
        )
      )
    ),
    TE.chain(({ messagingClient, client }) =>
      cleanupAndExit({ pgClient: client, kafkaClient: messagingClient })
    ),
    TE.orElse((error) => {
      log(() => `Error during the exit - ${error}`);
      // client and messaging client are not found
      // cleanupAndExit({ pgClient: client, kafkaClient: messagingClient });
      process.exit(1);
    })
  );
main();
