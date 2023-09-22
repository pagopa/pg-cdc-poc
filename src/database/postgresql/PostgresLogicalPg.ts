import * as E from "fp-ts/Either";
import * as TE from "fp-ts/TaskEither";
import { pipe } from "fp-ts/lib/function";
import { Wal2Json } from "pg-logical-replication";
import { AbstractPlugin } from "pg-logical-replication/dist/output-plugins/abstract.plugin";
import { DatabaseDeps } from "../../config/deps";

export type PgEvents = "start" | "data" | "error" | "acknowledge" | "heartbeat";

export const onDataEvent =
  (
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    listener: (...args: any[]) => Promise<void>
  ) =>
  ({ pgClient }: DatabaseDeps) =>
    pipe(
      TE.rightIO(() => {
        pgClient.pgLogicalClient.on(
          "data",
          (_: string, log: Wal2Json.Output) => {
            listener(log).catch((error) => {
              pgClient.pgLogicalClient.emit("error", error);
            });
          }
        );
      })
    );

export const subscribeToChanges =
  (plugin: AbstractPlugin, slotName: string) =>
  ({ pgClient }: DatabaseDeps) =>
    pipe(
      TE.tryCatch(
        () =>
          pgClient.pgLogicalClient
            .subscribe(plugin, slotName)
            .then(() => void 0),
        E.toError
      ),
      TE.mapLeft(
        (error) => new Error(`Error subscribing to slot ${slotName} - ${error}`)
      )
    );
