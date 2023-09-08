/* eslint-disable no-console */
import { defaultLog, useWinston, withConsole } from "@pagopa/winston-ts";
import * as TE from "fp-ts/TaskEither";
import { pipe } from "fp-ts/lib/function";
import { Client, ClientConfig } from "pg";
import { LogicalReplicationService } from "pg-logical-replication";
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
      pgLogicalClient: new LogicalReplicationService(config),
    }),
    (error) =>
      pipe(
        defaultLog.taskEither.error(`Error creating PG clients - ${error}`),
        () => new Error(`Error creating PG clients - ${error}`)
      )
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
