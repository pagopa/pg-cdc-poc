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
import { ClientConfig } from "pg";
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
import { transform } from "./mapping/customMapper";
import { Student } from "./model/student";

dotenv.config();
useWinston(withConsole());

const getPGConfig = (): E.Either<Error, ClientConfig> =>
  pipe(
    PostgreSQLConfig.decode(PGCONFIG),
    E.map((config) => ({
      host: config.HOST,
      port: config.PORT,
      database: config.DATABASE,
      user: config.USER,
      password: config.PASSWORD,
    })),
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

const processDBChanges =
  (client: KafkaProducerCompact<Student>) =>
  (lsn: string, messages: Student[]): TE.TaskEither<Error, void> =>
    pipe(
      transform(messages),
      sendMessages(client),
      // TE.chain(() => void ack(pgClient, lsn)()),
      // defaultLog.taskEither.info(
      //   `lsn ${lsn} - messages ${JSON.stringify(messages, (key, value) =>
      //     typeof value === "bigint" ? value.toString() : value
      //   )} - pgClient ${pgClient}`
      // ),
      TE.map(() => void 0),
      TE.mapLeft((error) =>
        pipe(
          defaultLog.taskEither.error(
            `Error during the message sending - ${error}`
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
    // defaultLog.taskEither.info("Starting import of the existing table..."),
    // TE.chainFirst(({ messagingClient }) =>
    //   pipe(
    //     getPGConfig(),
    //     TE.fromEither,
    //     defaultLog.taskEither.info("Config improted"),
    //     TE.chain((config) => queryStream(config)),
    //     TE.chain((messages) => sendMessages(messagingClient)(messages)),
    //     TE.fold(
    //       () => TE.of(void 0),
    //       (error) => pipe(defaultLog.taskEither.error("Error"), TE.of(error))
    //     ),
    //     defaultLog.taskEither.info("Import ended")
    //   )
    // ),
    // defaultLog.taskEither.info("Database imported"),
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

main().catch(defaultLog.either.error(`Application Error`));
