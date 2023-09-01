import * as TE from "fp-ts/TaskEither";
import { QueryResult } from "pg";
import { PGClient } from "./PostgresOperation";

export const query = (
  client: PGClient,
  queryString: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  values?: any[]
): TE.TaskEither<Error, QueryResult> =>
  TE.tryCatch(
    async () => await client.pgClient.query(queryString, values),
    (error) => new Error(`Error executing query - ${error}`)
  );
