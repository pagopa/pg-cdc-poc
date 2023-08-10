export interface MessagingClient {
  sendMessages(messages: any[], topic: string): Promise<void>;
  createTopic(topicName: string): void;
  connect(): Promise<void>;
  disconnect(): Promise<void>;
}
