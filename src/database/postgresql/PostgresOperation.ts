/* eslint-disable no-console */

import { defaultLog, useWinston, withConsole } from "@pagopa/winston-ts";
import * as TE from "fp-ts/TaskEither";
import { pipe } from "fp-ts/lib/function";
import { Client, ClientConfig, Pool } from "pg";
import { QueryIterablePool } from "pg-iterator";
import { LogicalReplicationService } from "pg-logical-replication";
import { Student } from "../../model/student";

useWinston(withConsole());

export type PGClient = {
  pgClient: Client;
  pgLogicalClient: LogicalReplicationService;
};

export const createPGClient = (
  config: ClientConfig
): TE.TaskEither<Error, PGClient> =>
  TE.tryCatch(
    async () => ({
      pgClient: new Client(config),
      pgLogicalClient: new LogicalReplicationService(config, {
        acknowledge: {
          auto: true,
          timeoutSeconds: 10,
        },
      }),
    }),
    (error) => new Error(`Error creating PG clients - ${error}`)
  );

export const connectPGClient = (client: PGClient): TE.TaskEither<Error, void> =>
  TE.tryCatch(
    async () => await client.pgClient.connect(),
    (error) =>
      pipe(
        defaultLog.taskEither.error(`Error connecting to PG - ${error}`),
        () => new Error(`Error connecting to PG - ${error}`)
      )
  );

export const disconnectPGClient = (
  client: PGClient
): TE.TaskEither<Error, void> =>
  TE.tryCatch(
    async () => await client.pgClient.end(),
    (error) => new Error(`Error disconnecting from PG - ${error}`)
  );

export const disconnectPGLogicalClient = (
  client: PGClient
): TE.TaskEither<Error, void> =>
  TE.tryCatch(
    async () => void (await client.pgLogicalClient.stop()),
    (error) => new Error(`Error disconnecting from PG - ${error}`)
  );

export const disconnectPGClientWithoutError = (
  client: PGClient
): TE.TaskEither<never, void> =>
  pipe(
    client,
    disconnectPGClient,
    TE.orElseW((_) => TE.right(undefined))
  );

export const queryStream = (
  config: ClientConfig
): TE.TaskEither<Error, Student[]> =>
  TE.tryCatch(
    async () => {
      const pool = new Pool(config);
      const client = new QueryIterablePool(pool);
      const stream = client.query("SELECT * FROM students");
      const students: Student[] = [];

      for await (const row of stream) {
        // eslint-disable-next-line functional/immutable-data
        students.push(row as Student);
      }
      return students;
    },
    (error) => new Error(`Error executing query - ${error}`)
  );
