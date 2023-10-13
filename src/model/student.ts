import * as t from "io-ts";

export const Student = t.type({
  id: t.number,
  firstName: t.string,
  lastName: t.string,
  dateOfBirth: t.string,
  status: t.number,
});

export type Student = t.TypeOf<typeof Student>;
