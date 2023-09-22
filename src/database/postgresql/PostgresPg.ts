import * as TE from "fp-ts/TaskEither";
import { DatabaseDeps } from "../../config/deps";

export const query =
  (
    queryString: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    values?: any[]
  ) =>
  ({ pgClient }: DatabaseDeps) =>
    TE.tryCatch(
      async () => await pgClient.pgClient.query(queryString, values),
      (error) => new Error(`Error executing query - ${error}`)
    );
