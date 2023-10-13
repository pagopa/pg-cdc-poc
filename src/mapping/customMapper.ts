/* eslint-disable @typescript-eslint/no-explicit-any */
import * as A from "fp-ts/lib/Array";
import * as O from "fp-ts/lib/Option";

import { useWinston, withConsole } from "@pagopa/winston-ts";
import { pipe } from "fp-ts/lib/function";
import { Student } from "../model/student";

type Transformer = (data: any) => Student[];
useWinston(withConsole());

const isValidTag = (tag: string) =>
  tag !== "commit" && tag !== "relation" && tag !== "begin";

const tagTransformer: Transformer = (data) =>
  pipe(
    O.fromNullable(data.tag),
    O.filter(isValidTag),
    O.chain(() => O.fromNullable(data.new)),
    O.fold(
      () => [],
      (newObject) => A.of(newObject as Student)
    )
  );

// const anotherTransformer: Transformer = (data) => { ... };
const transformers: Transformer[] = [
  tagTransformer /* , anotherTransformer, ... */,
];

export const transform = (messages: any[]): Student[] =>
  ([] as Student[]).concat(
    transformers.flatMap((transformer) => transformer(messages))
  );
