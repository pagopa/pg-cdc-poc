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
import { CONFIG, QUERIES, pgConfig, plugin } from "./config";
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
const log = withLogger(IO.io)(C.log);

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

const executeQuery = (
  client: PGClient,
  queryToExecute: string
): TE.TaskEither<Error, PGClient> =>
  pipe(
    query(client, queryToExecute),
    TE.map(() => client),
    TE.mapLeft((error) => error)
  );

const setupDatabase = (pgClient: PGClient): TE.TaskEither<Error, PGClient> =>
  pipe(
    executeQuery(pgClient, QUERIES.CREATE_TABLE),
    TE.chainFirst(() => executeQuery(pgClient, QUERIES.CREATE_PUBLICATION)),
    TE.chainFirst(() =>
      executeQuery(pgClient, QUERIES.CREATE_LOGICAL_REPLICATION_SLOT)
    )
  );

const processChanges =
  (client: KafkaProducerCompact<Student>) =>
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (messages: any[]): TE.TaskEither<Error, void> =>
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
    onDataEvent(dbClient, processChanges(messagingClient)),
    TE.chain(() =>
      subscribeToChanges(dbClient, plugin, CONFIG.POSTGRESQL.SLOT_NAME)
    )
  );

const handleSigInt = (clients: {
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

const waitForExit = (
  dbClient: PGClient,
  messagingClient: KafkaProducerCompact<Student>
): TE.TaskEither<Error, void | object> =>
  pipe(
    TE.Do,
    TE.chainFirst(() => TE.rightIO(IO.of(process.stdin.resume()))),
    TE.chainFirst(
      () =>
        void handleSigInt({
          pgClient: dbClient,
          kafkaClient: messagingClient,
        })
    )
  );

const main = () =>
  pipe(
    TE.Do,
    TE.bind("dbClient", () => createPGClient(pgConfig)),
    TE.bind("messagingClient", () => getSas()),
    TE.chainFirst(({ dbClient }) => connectPGClient(dbClient)),
    TE.chainFirst(({ dbClient }) => setupDatabase(dbClient)),
    TE.chainFirst(({ messagingClient, dbClient }) =>
      subscribeToDBChanges(dbClient, messagingClient)
    ),
    TE.chain(({ messagingClient, dbClient }) =>
      waitForExit(dbClient, messagingClient)
    ),
    TE.orElse(
      pipe(
        log(() => "Application failed"),
        process.exit(0)
      )
    )
  );
main();
