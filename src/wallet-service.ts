import { Service, Server, Processor, Config } from "hive-service";
import { server } from "./wallet-server";
import { processor } from "./wallet-processor";
import { listener as account_listener } from "./wallet-account-listener";
import { listener as transaction_listener } from "./wallet-transaction-listener";

const config: Config = {
  modname: "wallet",
  serveraddr: process.env["WALLET"],
  queueaddr: "ipc:///tmp/wallet.ipc",
  cachehost: process.env["CACHE_HOST"],
  dbhost: process.env["DB_HOST"],
  dbuser: process.env["DB_USER"],
  dbport: process.env["DB_PORT"],
  database: process.env["DB_NAME"],
  dbpasswd: process.env["DB_PASSWORD"],
  queuehost: process.env["QUEUE_HOST"],
  queueport: process.env["QUEUE_PORT"],
};

const svc: Service = new Service(config);

svc.registerServer(server);
svc.registerProcessor(processor);
svc.registerEventListener(account_listener);
svc.registerEventListener(transaction_listener);

svc.run();
