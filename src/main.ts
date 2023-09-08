/* eslint-disable no-console */
import { disconnect } from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaOperation";
import {
  AzureEventhubSasFromString,
  KafkaProducerCompact,
  fromSas,
  sendMessages,
} from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaProducerCompact";
import { defaultLog, useWinston, withConsole } from "@pagopa/winston-ts";
import dotenv from "dotenv";
import * as E from "fp-ts/Either";
import * as TE from "fp-ts/lib/TaskEither";
import { pipe } from "fp-ts/lib/function";
import { ClientConfig, QueryResult } from "pg";
import { EHCONFIG, PGCONFIG, plugin } from "./config/config";
import { PostgreSQLConfig } from "./config/ioConfig";
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
import { QUERIES } from "./utilities/query";

dotenv.config();
useWinston(withConsole());

const getPGConfig = (): E.Either<Error, ClientConfig> =>
  pipe(
    PostgreSQLConfig.decode(PGCONFIG),
    E.mapLeft((errors) => console.log(errors)),
    E.map((config) => {
      console.log(config);
      return {
        host: config.HOST,
        port: config.PORT,
        database: config.DATABASE,
        user: config.USER,
        password: config.PASSWORD,
      };
    }),
    E.mapLeft((errors) =>
      pipe(
        defaultLog.taskEither.error(
          `Error during decoding PG Config - ${errors}`
        ),
        () => new Error(`Error during decoding PG Config`)
      )
    )
  );

const getEventHubProducer = (): E.Either<
  Error,
  KafkaProducerCompact<Student>
> =>
  pipe(
    AzureEventhubSasFromString.decode(EHCONFIG.CONNECTION_STRING),
    E.map((sas) => fromSas(sas)),
    E.mapLeft((errors) =>
      pipe(
        defaultLog.taskEither.error(`Error decoding Event Hub SAS - ${errors}`),
        () => new Error(`Error decoding Event Hub SAS`)
      )
    )
  );

const setupDatabase = (pgClient: PGClient): TE.TaskEither<Error, QueryResult> =>
  pipe(
    query(pgClient, QUERIES.CREATE_TABLE),
    TE.chainFirst(() => query(pgClient, QUERIES.CREATE_PUBLICATION)),
    TE.chainFirst(() =>
      query(pgClient, QUERIES.CREATE_LOGICAL_REPLICATION_SLOT)
    )
  );

const processDBChanges =
  (client: KafkaProducerCompact<Student>) =>
  (messages: Student[]): TE.TaskEither<Error, void> =>
    pipe(
      transform(messages),
      sendMessages(client),
      TE.map(() => void 0),
      TE.mapLeft((errors) =>
        pipe(
          defaultLog.taskEither.error(
            `Error during the message sending - ${errors
              .map((error) => error.message)
              .join(", ")}`
          ),
          () => new Error(`Error during the message sending`)
        )
      )
    );

const subscribeToDBChanges = (
  dbClient: PGClient,
  messagingClient: KafkaProducerCompact<Student>
): TE.TaskEither<Error, void> =>
  pipe(
    onDataEvent(dbClient, processDBChanges(messagingClient)),
    TE.chain(() => subscribeToChanges(dbClient, plugin, PGCONFIG.SLOT_NAME))
  );

const waitForExit = (
  dbClient: PGClient,
  messagingClient: KafkaProducerCompact<Student>
): TE.TaskEither<Error, void> =>
  pipe(
    process.stdin.resume(),
    void process.on("SIGINT", () => {
      void cleanupAndExit({
        pgClient: dbClient,
        kafkaClient: messagingClient,
      })();
    })
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
      pipe(
        defaultLog.taskEither.info(
          "Disconnected from Database and Message Bus"
        ),
        process.exit(0)
      );
    }),
    TE.mapLeft((error) => {
      defaultLog.taskEither.error(`Error during the exit - ${error}`);
      process.exit(1);
    })
  );

const exitFromProcess = (): TE.TaskEither<Error, void | object> =>
  pipe(defaultLog.taskEither.error("Application failed"), process.exit(1));

const main = () =>
  pipe(
    TE.Do,
    defaultLog.taskEither.info(
      "Trying to connect to the event hub instance..."
    ),
    TE.bind("messagingClient", () => TE.fromEither(getEventHubProducer())),
    defaultLog.taskEither.info("Connected to the event hub instance"),
    defaultLog.taskEither.info("Creating PostgreSQl client..."),
    TE.bind("dbClient", () =>
      pipe(
        getPGConfig(),
        TE.fromEither,
        TE.chain((config) => createPGClient(config))
      )
    ),
    defaultLog.taskEither.info("Client created"),
    defaultLog.taskEither.info("Connecting to PostgreSQL..."),
    TE.chainFirst(({ dbClient }) => connectPGClient(dbClient)),
    defaultLog.taskEither.info("Connected to PostgreSQL"),
    defaultLog.taskEither.info("Initializing database..."),
    TE.chainFirst(({ dbClient }) => setupDatabase(dbClient)),
    defaultLog.taskEither.info("Database initialized"),
    defaultLog.taskEither.info("Subscribing to DB Changes..."),
    TE.chainFirst(({ messagingClient, dbClient }) =>
      subscribeToDBChanges(dbClient, messagingClient)
    ),
    defaultLog.taskEither.info("Subscribed to DB Changes"),
    defaultLog.taskEither.info(`Press CTRL+C to exit...Waiting...`),
    TE.chain(({ messagingClient, dbClient }) =>
      waitForExit(dbClient, messagingClient)
    )
  )();
TE.orElse(exitFromProcess);

main().catch(console.error);
