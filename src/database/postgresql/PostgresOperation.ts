/* eslint-disable no-console */
import { defaultLog, useWinston, withConsole } from "@pagopa/winston-ts";
import * as TE from "fp-ts/TaskEither";
import * as RTE from "fp-ts/ReaderTaskEither";
import { pipe } from "fp-ts/lib/function";
import { Client, ClientConfig } from "pg";
import { LogicalReplicationService } from "pg-logical-replication";
import { DatabaseDeps } from "../../config/deps";

useWinston(withConsole());

export type PGClient = {
  pgClient: Client;
  pgLogicalClient: LogicalReplicationService;
};

export const createPGClient =
  () =>
  ({ pgClientConfig }: { pgClientConfig: ClientConfig }) =>
    TE.tryCatch(
      async () => ({
        pgClient: new Client(pgClientConfig),
        pgLogicalClient: new LogicalReplicationService(pgClientConfig),
      }),
      (error) => new Error(`Error creating PG clients - ${error}`)
    );

export const connectPGClient =
  () =>
  ({ pgClient }: DatabaseDeps) =>
    TE.tryCatch(
      async () => await pgClient.pgClient.connect(),
      (error) =>
        pipe(
          defaultLog.taskEither.error(`Error connecting to PG - ${error}`),
          () => new Error(`Error connecting to PG - ${error}`)
        )
    );

export const disconnectPGClient =
  () =>
  ({ pgClient }: DatabaseDeps) =>
    TE.tryCatch(
      async () => await pgClient.pgClient.end(),
      (error) => new Error(`Error disconnecting from PG - ${error}`)
    );

export const disconnectPGLogicalClient =
  () =>
  ({ pgClient }: DatabaseDeps) =>
    TE.tryCatch(
      async () => void (await pgClient.pgLogicalClient.stop()),
      (error) => new Error(`Error disconnecting from PG - ${error}`)
    );

export const disconnectPGClientWithoutError = pipe(
  disconnectPGClient(),
  RTE.orElseW(() => RTE.right(undefined))
);
