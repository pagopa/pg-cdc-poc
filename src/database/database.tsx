
export interface DatabaseClient {
  connect(): void;
  disconnect(): void;
  createTable(): void;
  captureChanges(listener: (...args: any[]) => void): void;
}
