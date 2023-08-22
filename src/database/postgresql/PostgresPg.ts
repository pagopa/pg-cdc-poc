import * as TE from "fp-ts/TaskEither";
import { QueryResult } from "pg";
import { PGClient } from "./PostgresOperation";

export const query = (
  client: PGClient,
  queryString: string,
  values?: any[]
): TE.TaskEither<Error, QueryResult> =>
  TE.tryCatch(
    async () => {
      const result = await client.pgClient.query(queryString, values);
      console.log(`Executed query: ${queryString}`);
      return result;
    },
    (error) => {
      console.error(`Error executing query: ${queryString} - `, error);
      return new Error("Error executing query");
    }
  );
