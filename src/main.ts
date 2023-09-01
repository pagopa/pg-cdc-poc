import { disconnect } from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaOperation";
import {
  AzureEventhubSasFromString,
  KafkaProducerCompact,
  fromSas,
  sendMessages,
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
  connectPGClient,
  createPGClient,
  disconnectPGClient,
  disconnectPGLogicalClient,
} from "./database/postgresql/PostgresOperation";
import { query } from "./database/postgresql/PostgresPg";
import { transform } from "./mapping/customMapper";
import { Student } from "./model/student";

dotenv.config();

const CONFIG = {
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

const processChanges =
  (client: KafkaProducerCompact<Student>) =>
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (messages: any[]) =>
    pipe(
      transform(messages),
      sendMessages(client),
      TE.map(() => void 0),
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
      log(() => "Disconnected from Database and Message Bus.");
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

// const logStartup = TE.rightIO(
//   C.log("Startup Operations completed successfully.")
// );
const resumeStdin = TE.rightIO(IO.of(process.stdin.resume()));
const exitProcess = TE.rightIO(IO.of(process.exit(1)));

const handleSIGINT = (clients: {
  pgClient: PGClient;
  kafkaClient: KafkaProducerCompact<Student>;
}) =>
  TE.rightIO(
    IO.of(
      process.on("SIGINT", () => {
        void cleanupAndExit(clients)();
      })
    )
  );

const main = () =>
  pipe(
    TE.Do,
    TE.bind("pgClient", () => createPGClient(pgConfig)),
    TE.chainFirst(({ pgClient }) => connectPGClient(pgClient)),
    TE.bind("messagingClient", () => getSas()),
    TE.chainFirst(({ pgClient }) =>
      executeQuery(pgClient, QUERIES.CREATE_TABLE)
    ),
    TE.chainFirst(({ pgClient }) =>
      executeQuery(pgClient, QUERIES.CREATE_PUBLICATION)
    ),
    TE.chainFirst(({ pgClient }) =>
      executeQuery(pgClient, QUERIES.CREATE_LOGICAL_REPLICATION_SLOT)
    ),
    TE.chainFirst(({ messagingClient, pgClient }) =>
      pipe(
        onDataEvent(pgClient, processChanges(messagingClient)),
        TE.chain(() =>
          subscribeToChanges(pgClient, plugin, CONFIG.POSTGRESQL.SLOT_NAME)
        )
      )
    ),
    TE.chain(({ messagingClient, pgClient }) =>
      pipe(
        TE.Do,
        TE.chainFirst(() => resumeStdin),
        TE.chainFirst(
          () => void handleSIGINT({ pgClient, kafkaClient: messagingClient })
        )
      )
    ),
    TE.orElse(() =>
      pipe(
        TE.Do,
        TE.chainFirst(() => exitProcess)
      )
    )
  );
main();
