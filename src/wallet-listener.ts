import { BusinessEventContext, BusinessEventHandlerFunction, BusinessEventListener, rpc, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, set_for_response, waiting, msgpack_decode, msgpack_encode } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import { CustomerMessage } from "recommend-library";
import * as bluebird from "bluebird";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import * as http from "http";
import * as queryString from "querystring";
import * as Disq from "hive-disque";


export const listener = new BusinessEventListener("wallet-events-disque");
const wxhost = process.env["WX_ENV"] === "test" ? "dev.fengchaohuzhu.com" : "m.fengchaohuzhu.com";
const log = bunyan.createLogger({
  name: "wallet-listener",
  streams: [
    {
      level: "info",
      path: "/var/log/wallet-listener-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/wallet-listener-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

listener.onEvent(async function (ctx: BusinessEventContext, data: any) {
  
});