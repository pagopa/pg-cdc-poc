import { ServiceBusClient, ServiceBusSender } from "@azure/service-bus";
import { MessagingClient } from "./messaging";

class AzureServiceBusQueue implements MessagingClient {
  private sender: ServiceBusSender;
  constructor(connectionString: string, queueName: string) {
    const sbClient = new ServiceBusClient(connectionString);
    this.sender = sbClient.createSender(queueName);
  }
  connect(): Promise<void> {
    throw new Error("Method not implemented.");
  }
  disconnect(): Promise<void> {
    throw new Error("Method not implemented.");
  }
  async sendMessages(messages: any): Promise<void> {
    for (const message of messages) {
      await this.sender.sendMessages({ body: JSON.stringify(message) });
    }
  }

  async createTopic(topicName: string) {
    console.log("Creazione del topic: ", topicName);
  }
}

export default AzureServiceBusQueue;
