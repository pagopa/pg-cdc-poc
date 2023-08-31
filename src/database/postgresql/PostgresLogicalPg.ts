import * as E from "fp-ts/Either";
import * as TE from "fp-ts/TaskEither";
import { pipe } from "fp-ts/lib/function";
import { Wal2Json } from "pg-logical-replication";
import { AbstractPlugin } from "pg-logical-replication/dist/output-plugins/abstract.plugin";
import { PGClient } from "./PostgresOperation";

export type PgEvents = "start" | "data" | "error" | "acknowledge" | "heartbeat";

export const onDataEvent = (
  client: PGClient,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  listener: (...args: any[]) => TE.TaskEither<Error, void>
): TE.TaskEither<Error, void> =>
  pipe(
    TE.rightIO(() => {
      client.pgLogicalClient.on("data", (lsn: string, log: Wal2Json.Output) => {
        void listener(log)();
      });
    }),
    TE.mapLeft(
      (error) => new Error(`Error during data event subscription - ${error}`)
    )
  );

export const subscribeToChanges = (
  client: PGClient,
  plugin: AbstractPlugin,
  slotName: string
): TE.TaskEither<Error, void> =>
  pipe(
    TE.tryCatch(async () => {
      void client.pgLogicalClient.subscribe(plugin, slotName);
    }, E.toError),
    TE.mapLeft(
      (error) => new Error(`Error subscribing to slot ${slotName} - ${error}`)
    )
  );
