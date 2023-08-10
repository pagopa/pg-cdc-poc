import dotenv from "dotenv";
import { DatabaseClient } from "./database/database";
import PostgreSQLClient from "./database/postgresql";
import { CustomMapper } from "./mapping/customMapper";
import KafkaClient from "./messaging/kafka";
import { MessagingClient } from "./messaging/messaging";

dotenv.config();
const KAFKA_CONNECTION_SRING = process.env.KAFKA_CONNECTION_SRING || "localhost:9092";
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "postgresql-topic";
const KAFKA_APP_ID = process.env.KAFKA_APP_ID || "postgresql-app";

const POSTGRESQL_CONNECTION_STRING = process.env.POSTGRESQL_CONNECTION_STRING || "postgres://postgres:postgres@localhost:5432/cdc_test";
export const POSTGRESQL_PUBLICATION_NAMES =
  process.env.POSTGRESQL_PUBLICATION_NAMES || "students";
export const POSTGRESQL_SLOT_NAME = process.env.POSTGRESQL_SLOT_NAME || "slot_students";

async function main() {
  const messagingClient: MessagingClient = new KafkaClient(
    [KAFKA_CONNECTION_SRING],
    KAFKA_APP_ID
  );

  const dbClient: DatabaseClient = new PostgreSQLClient(
    POSTGRESQL_CONNECTION_STRING,
    POSTGRESQL_PUBLICATION_NAMES,
    POSTGRESQL_SLOT_NAME
  );

  try {
    console.log("Starting the application...");
    await messagingClient.connect();
    await messagingClient.createTopic(KAFKA_TOPIC);

    await dbClient.connect();
    await dbClient.createTable();
    await dbClient.captureChanges(async (messages: any[]) => {
      try {
        const mapper = new CustomMapper(messages);
        const message = mapper.transform();
        if (message && message.length > 0) {
          messagingClient.sendMessages([message], KAFKA_TOPIC);
        }
      } catch (error) {
        console.error("Error occurred:", error);
      }
    });
  } catch (error) {
    console.error("Error occurred:", error);
  } finally {
    process.stdin.resume();
    process.on("SIGINT", async function () {
      console.log("Received SIGINT (Ctrl+C). Exiting...");
      await dbClient.disconnect();
      await messagingClient.disconnect();
      process.exit(0);
    });
  }
}

main().catch((error) => {
  console.error("An unexpected error occurred:", error);
});
