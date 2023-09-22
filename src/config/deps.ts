import { KafkaProducerCompact } from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaProducerCompact";
import { PGClient } from "../database/postgresql/PostgresOperation";
import { Student } from "../model/student";

export interface QueueDeps {
  messagingClient: KafkaProducerCompact<Student>;
}

export interface DatabaseDeps {
  pgClient: PGClient;
}

export interface Deps extends QueueDeps, DatabaseDeps {}
