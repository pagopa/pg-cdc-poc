import { AzureEventhubSasFromString } from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaProducerCompact";
import * as t from "io-ts";
import * as ts from "io-ts-types";

export const PostgreSQLConfig = t.type({
  HOST: ts.NonEmptyString,
  PORT: t.Int,
  DATABASE: ts.NonEmptyString,
  USER: ts.NonEmptyString,
  PASSWORD: ts.NonEmptyString,
  PUBLICATION_NAMES: ts.NonEmptyString,
  SLOT_NAME: ts.NonEmptyString,
});

export const EventHubConfig = t.type({
  CONNECTION_STRING: AzureEventhubSasFromString,
});
