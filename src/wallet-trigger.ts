import { rpcAsync, msgpack_decode_async, msgpack_encode_async, Result } from "hive-service";
import * as bluebird from "bluebird";
import * as msgpack from "msgpack-lite";
import * as bunyan from "bunyan";
import { createClient, RedisClient, Multi } from "redis";
import { Socket, socket } from "nanomsg";
import { OrderEvent, OrderEventType, PlanOrder, AdditionalOrder, AdditionalOrderEventType, AdditionalOrderEvent } from "order-library";
import { User } from "profile-library";

const log = bunyan.createLogger({
  name: "wallet-trigger",
  streams: [
    {
      level: "info",
      path: "/var/log/wallet-trigger-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/wallet-trigger-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

export function run () {
  const cache: RedisClient = bluebird.promisifyAll(createClient(process.env["CACHE_PORT"], process.env["CACHE_HOST"], { "return_buffers": true })) as RedisClient;

  const socket1: Socket = socket("sub");
  socket1.connect(process.env["PLAN-ORDER-EVENT-TRIGGER"]);
  socket1.on("data", function (buf) {
    const event: OrderEvent = msgpack.decode(buf) as OrderEvent;
    log.info(`Got order event (${JSON.stringify(event)})`);
    (async () => {
      switch(event.type) {
        case OrderEventType.PAY: {
          const oresult: Result<any> = await rpcAsync<any>("mobile", process.env["WALLET"], event.opid, "rechargePlanOrder", event.oid);
          log.info(`recharge result: ${oresult.code}, ${oresult.data}, ${oresult.msg}`);
          break;
        }
        case OrderEventType.CANCEL: {
          break;
        }
        default: break;
      }
    })();
  });

  const socket2: Socket = socket("sub");
  socket2.connect(process.env["ADDITIONAL-ORDER-EVENT-TRIGGER"]);
  socket2.on("data", function (buf) {
    const event: AdditionalOrderEvent = msgpack.decode(buf) as AdditionalOrderEvent;
    log.info(`Got additional order event (${JSON.stringify(event)})`);
    (async () => {
      switch(event.type) {
        case AdditionalOrderEventType.PAY: {
          log.info(process.env["WALLET"]);
          const oresult: Result<any> = await rpcAsync<any>("mobile", process.env["WALLET"], event.opid, event.project === 2 ? "rechargeThirdOrder" : "rechargeDeathOrder", event.oid);
          log.info(`${event.project === 2 ? "rechargeThirdOrder" : "rechargeDeathOrder"} result: ${oresult.code}, ${oresult.data}, ${oresult.msg}`);
          break;
        }
        default: break;
      }
    })();
  });


  log.info(`wallet-trigger is running on ${process.env["PLAN-ORDER-EVENT-TRIGGER"]} and ${process.env["ADDITIONAL-ORDER-EVENT-TRIGGER"]}`);
}
