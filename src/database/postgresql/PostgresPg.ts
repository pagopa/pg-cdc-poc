import * as RTE from "fp-ts/ReaderTaskEither";
import * as TE from "fp-ts/TaskEither";
import { QueryResult } from "pg";
import { PGClient } from "./PostgresOperation";

export const query =
  (
    queryString: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    values?: any[]
  ): RTE.ReaderTaskEither<{ pgClient: PGClient }, Error, QueryResult> =>
  ({ pgClient }) =>
    TE.tryCatch(
      async () => await pgClient.pgClient.query(queryString, values),
      (error) => new Error(`Error executing query - ${error}`)
    );
