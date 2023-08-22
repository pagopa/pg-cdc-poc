import * as E from "fp-ts/Either";
import * as TE from "fp-ts/TaskEither";
import { pipe } from "fp-ts/lib/function";
import { Wal2Json } from "pg-logical-replication";
import { AbstractPlugin } from "pg-logical-replication/dist/output-plugins/abstract.plugin";
import { PGClient } from "./PostgresOperation";

export type PgEvents = "start" | "data" | "error" | "acknowledge" | "heartbeat";

export const onDataEvent = (
  client: PGClient,
  listener: (...args: any[]) => TE.TaskEither<Error, void>
): TE.TaskEither<Error, void> => {
  return pipe(
    TE.rightIO(() => {
      console.log("Attempting to capture changes on data...");
      client.pgLogicalClient.on("data", (lsn: string, log: Wal2Json.Output) => {
        console.log("Data captured");
        listener(log)();
      });
    }),
    TE.map(() => console.log("Data event subscribed successfully.")),
    TE.mapLeft((error) => {
      console.error("Error during data event subscription:", error);
      return new Error("Error during data event subscription");
    })
  );
};

export const subscribeToChanges = (
  client: PGClient,
  plugin: AbstractPlugin,
  slotName: string
): TE.TaskEither<Error, void> => {
  return pipe(
    TE.tryCatch(async () => {
      console.log(`Attempting to subscribe to publication: ${slotName}...`);
      client.pgLogicalClient.subscribe(plugin, slotName);
    }, E.toError),
    TE.map(() => console.log(`Successfully subscribed to slot: ${slotName}`)),
    TE.mapLeft((error) => {
      console.error(`Error subscribing to slot ${slotName}:`, error);
      return error;
    })
  );
};
