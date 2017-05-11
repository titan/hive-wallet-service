import { rpcAsync, msgpack_decode_async, msgpack_encode_async, Result } from "hive-service";
import * as bluebird from "bluebird";
import * as msgpack from "msgpack-lite";
import * as bunyan from "bunyan";
import { createClient, RedisClient, Multi } from "redis";
import { Socket, socket } from "nanomsg";
import { OrderEvent, OrderEventType, PlanOrder } from "order-library";
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

  const mobile_socket: Socket = socket("sub");
  mobile_socket.connect(process.env["ORDER-EVENT-TRIGGER"]);
  mobile_socket.on("data", function (buf) {
    const event: OrderEvent = msgpack.decode(buf) as OrderEvent;
    log.info(`Got order event (${JSON.stringify(event)})`);
    (async () => {
      switch(event.type) {
        case OrderEventType.PAY: {
          const oresult: Result<any> = await rpcAsync<any>("mobile", process.env["wallet"], event.opid, "recharge", event.oid);
          break;
        }
        case OrderEventType.CANCEL: {
          break;
        }
        default: break;
      }
    })();
  });
  log.info(`wallet-trigger is running on ${process.env["ORDER-EVENT-TRIGGER"]}`);
}
