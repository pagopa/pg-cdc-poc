/* eslint-disable no-console */
import { defaultLog, useWinston, withConsole } from "@pagopa/winston-ts";
import * as TE from "fp-ts/TaskEither";
import * as RTE from "fp-ts/ReaderTaskEither";
import { pipe } from "fp-ts/lib/function";
import { Client, ClientConfig } from "pg";
import { LogicalReplicationService } from "pg-logical-replication";

useWinston(withConsole());

export type PGClient = {
  pgClient: Client;
  pgLogicalClient: LogicalReplicationService;
};

export const createPGClient =
  (): RTE.ReaderTaskEither<{ pgClientConfig: ClientConfig }, Error, PGClient> =>
  ({ pgClientConfig }) =>
    TE.tryCatch(
      async () => ({
        pgClient: new Client(pgClientConfig),
        pgLogicalClient: new LogicalReplicationService(pgClientConfig),
      }),
      (error) => new Error(`Error creating PG clients - ${error}`)
    );

export const connectPGClient =
  (): RTE.ReaderTaskEither<{ pgClient: PGClient }, Error, void> =>
  ({ pgClient }) =>
    TE.tryCatch(
      async () => await pgClient.pgClient.connect(),
      (error) =>
        pipe(
          defaultLog.taskEither.error(`Error connecting to PG - ${error}`),
          () => new Error(`Error connecting to PG - ${error}`)
        )
    );

export const disconnectPGClient =
  (): RTE.ReaderTaskEither<{ pgClient: PGClient }, Error, void> =>
  ({ pgClient }) =>
    TE.tryCatch(
      async () => await pgClient.pgClient.end(),
      (error) => new Error(`Error disconnecting from PG - ${error}`)
    );

export const disconnectPGLogicalClient =
  (): RTE.ReaderTaskEither<{ pgClient: PGClient }, Error, void> =>
  ({ pgClient }) =>
    TE.tryCatch(
      async () => void (await pgClient.pgLogicalClient.stop()),
      (error) => new Error(`Error disconnecting from PG - ${error}`)
    );

export const disconnectPGClientWithoutError = pipe(
  disconnectPGClient(),
  RTE.orElseW(() => RTE.right(undefined))
);
