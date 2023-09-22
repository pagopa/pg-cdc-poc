import { disconnect } from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaOperation";
import {
  AzureEventhubSasFromString,
  KafkaProducerCompact,
  fromSas,
  sendMessages,
} from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaProducerCompact";
import dotenv from "dotenv";
import * as E from "fp-ts/Either";
import * as TE from "fp-ts/TaskEither";
import * as RTE from "fp-ts/lib/ReaderTaskEither";
import { constVoid, pipe } from "fp-ts/lib/function";
import { ClientConfig } from "pg";
import { ValidationError } from "io-ts";
import { log, info, error } from "fp-ts/lib/Console";
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

const logRTE = {
  info: (msg: string) => () => RTE.fromIO(info(msg)),
  error: (msg: string) => () => RTE.fromIO(error(msg)),
  log: (msg: string) => () => RTE.fromIO(log(msg)),
};

const getPGConfig = (): E.Either<ValidationError[], ClientConfig> =>
  pipe(
    PostgreSQLConfig.decode(PGCONFIG),
    E.map((config) => ({
      host: config.HOST,
      port: config.PORT,
      database: config.DATABASE,
      user: config.USER,
      password: config.PASSWORD,
    }))
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
        log(`Error decoding Event Hub SAS - ${errors}`),
        () => new Error(`Error decoding Event Hub SAS`)
      )
    )
  );

const setupDatabase = () =>
  pipe(
    query(QUERIES.CREATE_TABLE),
    RTE.chainFirst(() => query(QUERIES.CREATE_PUBLICATION)),
    RTE.chainFirst(() => query(QUERIES.CREATE_LOGICAL_REPLICATION_SLOT))
  );

const dbChangesListener =
  (): RTE.ReaderTaskEither<
    { messagingClient: KafkaProducerCompact<Student> },
    Error,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (...args: any[]) => Promise<void>
  > =>
  ({ messagingClient }) =>
    TE.of((messages: Student[]) =>
      pipe(
        transform(messages),
        sendMessages(messagingClient),
        TE.map(constVoid),
        TE.mapLeft((errors) =>
          pipe(
            error(
              `Error during the message sending - ${errors
                .map((error) => error.message)
                .join(", ")}`
            ),
            constVoid
          )
        ),
        TE.toUnion
      )()
    );

const subscribeToDBChanges = () =>
  pipe(
    RTE.Do,
    RTE.bindW("listener", dbChangesListener),
    RTE.chainW(({ listener }) => onDataEvent(listener)),
    RTE.chain(() => subscribeToChanges(plugin, PGCONFIG.SLOT_NAME))
  );

const waitForExit =
  (): RTE.ReaderTaskEither<
    {
      dbClient: PGClient;
      messagingClient: KafkaProducerCompact<Student>;
    },
    Error,
    void
  > =>
  ({ dbClient, messagingClient }) =>
    pipe(
      process.stdin.resume(),
      void process.on("SIGINT", () => {
        void cleanupAndExit({
          pgClient: dbClient,
          kafkaClient: messagingClient,
        })();
      })
    );

const disconnectKafkaProducer =
  (): RTE.ReaderTaskEither<
    {
      kafkaClient: KafkaProducerCompact<Student>;
    },
    Error,
    void
  > =>
  ({ kafkaClient }) =>
    pipe(
      kafkaClient,
      TE.fromIO,
      TE.chain((client) => disconnect(client.producer))
    );

const cleanupAndExit = pipe(
  disconnectPGLogicalClient(),
  RTE.chainFirst(() => query(QUERIES.DROP_PUBLICATION)),
  RTE.chainFirst(() => query(QUERIES.DROP_LOGICAL_REPLICATION_SLOT)),
  RTE.chainFirst(disconnectPGClient),
  RTE.chainFirstW(disconnectKafkaProducer),
  RTE.chainFirst(() => {
    info("Disconnected from Database and Message Bus");
    process.exit(0);
  }),
  RTE.mapLeft((e) => {
    error(`Error during the exit - ${e}`);
    process.exit(1);
  })
);

// const exitFromProcess = (): TE.TaskEither<Error, void | object> =>
//   pipe(defaultLog.taskEither.error("Application failed"), process.exit(1));

const main = pipe(
  RTE.Do,
  RTE.chainFirstW(
    logRTE.info("Trying to connect to the event hub instance...")
  ),
  RTE.bind("messagingClient", () => RTE.fromEither(getEventHubProducer())),
  RTE.chainFirstW(logRTE.info("Connected to the event hub instance")),
  RTE.chainFirstW(logRTE.info("Creating PostgreSQl client...")),
  RTE.bind("pgClient", createPGClient),
  RTE.map(
    pipe(
      RTE.of(logRTE.info("Client created")),
      RTE.chainFirstW(logRTE.info("Connecting to PostgreSQL...")),
      RTE.chainFirstW(connectPGClient),
      RTE.chainFirstW(logRTE.info("Connected to PostgreSQL")),
      RTE.chainFirstW(logRTE.info("Initializing database...")),
      RTE.chainFirstW(setupDatabase),
      RTE.chainFirstW(logRTE.info("Database initialized")),
      RTE.chainFirstW(logRTE.info("Subscribing to DB Changes...")),
      RTE.chainFirstW(subscribeToDBChanges),
      RTE.chainFirstW(logRTE.info("Subscribed to DB Changes")),
      RTE.chainFirstW(logRTE.info(`Press CTRL+C to exit...Waiting...`)),
      RTE.chainFirstW(waitForExit)
    )
  )
);

const run = () =>
  pipe(
    getPGConfig(),
    E.foldW(
      (errors) =>
        pipe(
          error(`Error during decoding PG Config - ${errors}`),
          () => new Error(`Error during decoding PG Config`)
        ),
      (pgClientConfig) =>
        main({
          pgClientConfig,
        })().catch(error(`Application Error`))
    )
  );

void run();
