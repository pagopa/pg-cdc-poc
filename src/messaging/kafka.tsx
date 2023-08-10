import { Admin, Kafka, Producer, logLevel } from "kafkajs";
import { MessagingClient } from "./messaging";

class KafkaClient implements MessagingClient {
  private client: Kafka;
  private admin: Admin;
  private producer: Producer;

  constructor(connectionString: string[], clientId: string) {
    this.client = new Kafka({
      clientId: clientId,
      brokers: connectionString,
      logLevel: logLevel.ERROR,
    });
    this.admin = this.client.admin();
    this.producer = this.client.producer();
  }

  public async connect(): Promise<void> {
    console.log("Connecting to Kafka...");
    await this.producer.connect();
    await this.admin.connect();
    console.log("Connected to Kafka successfully.");
  }

  public async disconnect(): Promise<void> {
    console.log("Disconnecting from Kafka...");
    await this.producer.disconnect();
    await this.admin.disconnect();
    console.log("Disconnected from Kafka successfully.");
  }

  async createTopic(topicName: string) {
    console.log("Checking if topic ", topicName, " exists");
    const topics = await this.admin.listTopics();
    console.log("Existing Topics : ", topics);
    if (!topics.includes(topicName)) {
      console.log("Topic ", topicName, "not exists, creating it");
      await this.admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic: topicName,
            numPartitions: 3,
          },
        ],
      });
      console.log("Topic created succesfully");
    }
  }

  async sendMessages(messages: any[], topic: string): Promise<void> {
    console.log("Sending messages... ");
    for (const message of messages) {
      console.log("Sending message: ", message, "on topic: ", topic);
      await this.producer.send({
        topic: topic,
        messages: [{ value: JSON.stringify(message) }],
      });
    }
    console.log("Message sent");
  }
}

export default KafkaClient;
