import { pipe } from "fp-ts/lib/function";
import * as TE from "fp-ts/TaskEither";
import { Client, ClientConfig } from "pg";
import { LogicalReplicationService } from "pg-logical-replication";

export type PGClient = {
  pgClient: Client;
  pgLogicalClient: LogicalReplicationService;
};

export const createPGClient = (
  config: ClientConfig
): TE.TaskEither<Error, PGClient> => {
  return TE.tryCatch(
    async () => {
      const pgClient = new Client(config);
      const logicalRepClient = new LogicalReplicationService(config);
      console.log("Clients created successfully");
      return { pgClient: pgClient, pgLogicalClient: logicalRepClient };
    },
    (error) => {
      console.error("Error creating PG clients:", error);
      return new Error("Error creating PG clients");
    }
  );
};

export const connectPGClient = (client: PGClient): TE.TaskEither<Error, void> =>
  TE.tryCatch(
    async () => {
      await client.pgClient.connect();
      console.log("PG clients connected successfully");
    },
    (error) => {
      console.error("Error connecting to PG:", error);
      return new Error("Error connecting to PG");
    }
  );

export const disconnectPGClient = (
  client: PGClient
): TE.TaskEither<Error, void> =>
  TE.tryCatch(
    async () => {
      await client.pgClient.end();
      console.log("Disconnected from PG successfully");
    },
    (error) => {
      console.error("Error disconnecting from PG:", error);
      return new Error("Error disconnecting from PG");
    }
  );

  export const disconnectPGLogicalClient = (
    client: PGClient
  ): TE.TaskEither<Error, void> =>
    TE.tryCatch(
      async () => {
        await client.pgLogicalClient.stop();
        console.log("Disconnected from PG successfully");
      },
      (error) => {
        console.error("Error disconnecting from PG:", error);
        return new Error("Error disconnecting from PG");
      }
    );

export const disconnectPGClientWithoutError = (
  client: PGClient
): TE.TaskEither<never, void> =>
  pipe(
    client,
    disconnectPGClient,
    TE.orElseW((_) => {
      console.log("Disconnecting from PG with suppressed error");
      return TE.right(undefined);
    })
  );
