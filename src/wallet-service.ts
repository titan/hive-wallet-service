import { Service, Server, Processor, Config } from "hive-service";
import { server } from "./wallet-server";
import { processor } from "./wallet-processor";
import { listener as account_listener } from "./wallet-account-listener";
import { listener as transaction_listener } from "./wallet-transaction-listener";
import * as bunyan from "bunyan";

const log = bunyan.createLogger({
  name: "wallet-service",
  streams: [
    {
      level: "info",
      path: "/var/log/wallet-service-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/wallet-service-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

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
  loginfo: (...x) => log.info(x),
  logerror: (...x) => log.error(x),
};

const svc: Service = new Service(config);

svc.registerServer(server);
svc.registerProcessor(processor);
svc.registerEventListener(account_listener);
svc.registerEventListener(transaction_listener);

svc.run();
