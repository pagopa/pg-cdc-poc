import {
  AzureEventhubSas,
  AzureEventhubSasFromString,
} from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaProducerCompact";
import * as t from "io-ts";
import * as ts from "io-ts-types";
import { ClientConfig } from "pg";

const PostgreSQLConfig = t.type({
  HOST: ts.NonEmptyString,
  PORT: ts.IntFromString,
  DATABASE: ts.NonEmptyString,
  USER: ts.NonEmptyString,
  PASSWORD: ts.NonEmptyString,
  PUBLICATION_NAMES: ts.NonEmptyString,
  SLOT_NAME: ts.NonEmptyString,
});

export const EventHubConfig = t.type({
  CONNECTION_STRING: AzureEventhubSasFromString,
});

export const Config = t.type({
  POSTGRESQL: PostgreSQLConfig,
  EVENTHUB: EventHubConfig,
});

export type IConfig = t.TypeOf<typeof Config>;

export type Config = {
  dbConfig: ClientConfig;
  messagingConfig: AzureEventhubSas;
};
