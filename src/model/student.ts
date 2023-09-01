import * as t from "io-ts";
import { DateFromISOString } from "io-ts-types/DateFromISOString";

export const Student = t.type({
  id: t.number,
  firstName: t.string,
  lastName: t.string,
  dateOfBirth: DateFromISOString,
});

export type Student = t.TypeOf<typeof Student>;
