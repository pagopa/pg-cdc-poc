import { Client } from "pg";
import {
  LogicalReplicationService,
  PgoutputPlugin,
  Wal2Json,
} from "pg-logical-replication";
import { AbstractPlugin } from "pg-logical-replication/dist/output-plugins/abstract.plugin";
import { DatabaseClient } from "./database";

class PostgreSQLClient implements DatabaseClient {
  private client: LogicalReplicationService;
  private pgClient: Client;
  private connectionString: string;
  private publicationName: string;
  private slotName: string;

  constructor(
    connectionString: string,
    publicationName: string,
    slotName: string
  ) {
    this.connectionString = connectionString;
    this.pgClient = new Client({ connectionString: this.connectionString });
    this.publicationName = publicationName;
    this.slotName = slotName;
  }

  async connect() {
    console.log("Trying to connect to the database ", this.connectionString);
    this.client = new LogicalReplicationService(
      {
        connectionString: this.connectionString,
      },
      {
        acknowledge: {
          auto: true,
          timeoutSeconds: 10,
        },
      }
    );
    await this.pgClient.connect();
    console.log("Connected to the database");
    this.createPublication();
    this.createSlot();
  }

  async captureChanges(listener: (...args: any[]) => void): Promise<void> {
    console.log("Setting listener to data changes...");
    this.client.on("data", (lsn: string, log: Wal2Json.Output) => {
      listener(log);
    });

    this.client.subscribe(
      new PgoutputPlugin({
        protoVersion: 1,
        publicationNames: [this.publicationName],
      }),
      this.slotName
    );
  }

  subscribe(plugin: AbstractPlugin<any>, slotName: string) {
    console.log(
      "Subscribing to the CDC publication with plugin",
      JSON.stringify(plugin),
      " on slot ",
      slotName
    );

    this.client
      .subscribe(plugin, slotName)
      .catch((e: Error) => {
        console.error("Error subscribing to the CDC publication ", e.message);
      })
      .then(() => {
        setTimeout(this.subscribe, 100);
      });

    console.log("Subscribed to the CDC publication");
  }

  async createTable(): Promise<void> {
    console.log("Creating table STUDENTS if not exists");
    await this.pgClient.query(
      `CREATE TABLE IF NOT EXISTS students (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        date_of_birth DATE NOT NULL
    );`
    );
    console.log("Table created");
  }
  async createPublication(): Promise<void> {
    console.log(`Creating publication ${this.publicationName}`);
    await this.pgClient.query(
      `CREATE PUBLICATION ${this.publicationName} FOR ALL TABLES;`
    );
  }
  async dropPublication(): Promise<void> {
    console.log(`Dropping publication ${this.publicationName}`);
    await this.pgClient.query(`DROP PUBLICATION ${this.publicationName};`);
  }

  async dropSlot(): Promise<void> {
    console.log(`Dropping slot ${this.slotName}`);
    const result = await this.pgClient.query(
      `SELECT pg_drop_replication_slot('${this.slotName}');
      `
    );
    console.log("result ", result);
  }

  async createSlot(): Promise<void> {
    console.log(`Creating slot ${this.slotName}`);
    await this.pgClient.query(
      `SELECT pg_create_logical_replication_slot('${this.slotName}', 'pgoutput');
      `
    );
  }

  async disconnect() {
    try {
      console.log("Disconnecting from the database...");
      await this.client.stop();
      await this.dropPublication();
      await this.dropSlot();
      await this.pgClient.end();
      console.log("Disconnected from the database");
    } catch (e) {
      console.error(
        "Error disconnecting from the database ",
        JSON.stringify(e)
      );
    }
  }
}

export default PostgreSQLClient;
