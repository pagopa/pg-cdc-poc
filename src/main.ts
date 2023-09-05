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
import { ClientConfig, QueryResult } from "pg";
import { EHCONFIG, PGCONFIG, plugin } from "./config/config";
import { Config } from "./config/ioConfig";
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
const log = withLogger(IO.io)(C.log);

const getPGConfig = (): E.Either<Error, ClientConfig> =>
  pipe(
    Config.decode(PGCONFIG),
    E.map((config) => ({
      host: config.POSTGRESQL.HOST,
      port: config.POSTGRESQL.PORT,
      database: config.POSTGRESQL.DATABASE,
      user: config.POSTGRESQL.USER,
      password: config.POSTGRESQL.PASSWORD,
    })),
    E.mapLeft(
      (errors) => new Error(`Error during decoding PG Config - ${errors}`)
    )
  );

const getEHConfig = (): E.Either<Error, KafkaProducerCompact<Student>> =>
  pipe(
    AzureEventhubSasFromString.decode(EHCONFIG.CONNECTION_STRING),
    E.map((sas) => fromSas(sas)),
    E.mapLeft(
      (errors) => new Error(`Error during decoding Event Hub SAS - ${errors}`)
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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (messages: Student[]): TE.TaskEither<Error, void> =>
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
        log(() => "Disconnected from Database and Message Bus."),
        process.exit(0)
      );
    }),
    TE.mapLeft((error) => {
      log(() => `Error during the exit - ${error}`);
      process.exit(1);
    })
  );

const exitFromProcess = (): TE.TaskEither<Error, void | object> =>
  pipe(
    log(() => "Application failed"),
    process.exit(1)
  );

const main = () =>
  pipe(
    getPGConfig(),
    TE.fromEither,
    TE.chain((config) =>
      pipe(
        TE.Do,
        TE.bind("dbClient", () => createPGClient(config)),
        TE.chainFirst(({ dbClient }) => connectPGClient(dbClient)),
        TE.chainFirst(({ dbClient }) => setupDatabase(dbClient)),
        TE.bind("messagingClient", () => TE.fromEither(getEHConfig())),
        TE.chainFirst(({ messagingClient, dbClient }) =>
          subscribeToDBChanges(dbClient, messagingClient)
        ),
        TE.chain(({ messagingClient, dbClient }) =>
          waitForExit(dbClient, messagingClient)
        )
      )
    ),
    TE.orElse(exitFromProcess)
  );

main();
