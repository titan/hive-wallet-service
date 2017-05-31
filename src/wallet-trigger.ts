import { rpcAsync, msgpack_decode_async, msgpack_encode_async, Result } from "hive-service";
import * as bluebird from "bluebird";
import * as msgpack from "msgpack-lite";
import * as bunyan from "bunyan";
import { createClient, RedisClient, Multi } from "redis";
import { Socket, socket } from "nanomsg";
import { OrderEvent, OrderEventType, PlanOrder, AdditionalOrder, AdditionalOrderEventType, AdditionalOrderEvent } from "order-library";
import { User } from "profile-library";
import { Wallet } from "wallet-library";
import { additionalPaySuccess } from "wechat-library";

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
          log.info(`recharge result: ${oresult.code}, ${JSON.stringify(oresult.data)}, ${oresult.msg}`);
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
          const oresult: Result<Wallet> = await rpcAsync<Wallet>("mobile", process.env["WALLET"], event.opid, event.project === 2 ? "rechargeThirdOrder" : "rechargeDeathOrder", event.oid);
          log.info(`${event.project === 2 ? "rechargeThirdOrder" : "rechargeDeathOrder"} result: ${oresult.code}, ${JSON.stringify(oresult.data)}, ${oresult.msg}`);
          if (oresult.code === 200) {
            const wallet: Wallet = oresult.data;
            const presult: Result<User> = await rpcAsync<User>("admin", process.env["PROFILE"], event.opid, "getUser", event.opid);
            if (presult.code === 200) {
              const user: User = presult.data;
              const aoresult: Result<AdditionalOrder> = await rpcAsync<AdditionalOrder>("admin", process.env["ORDER"], event.opid, "getAdditionalOrder", event.oid);
              if (aoresult.code === 200) {
                const order: AdditionalOrder = aoresult.data;
                for (const account of wallet.accounts) {
                  if (account.license === order.license_no) {
                    const new_balance = account.balance1 / 100;
                    const balance = new_balance - event.payment;
                    log.info(`additionalPaySuccess(openid: "${user.openid}", license: "${order.license_no}", amount: ${event.payment}, balance: ${balance}, new_balance: ${new_balance}, time: "${event.occurred_at.toISOString()}")`);
                    const response = await additionalPaySuccess(user.openid, order.license_no, event.payment, balance, new_balance, event.occurred_at);
                    log.info(`send wechat notification for ${event.project === 2 ? "rechargeThirdOrder" : "rechargeDeathOrder"}(${event.oid}), got response: ${response}`);
                    return;
                  }
                }
                log.info(`Cannot found account to send wechat notification for additional order ${event.oid}`);
              } else {
                log.info(`Cannot found additional order ${event.oid} to send wechat notification with result: ${JSON.stringify(aoresult)}`);
              }
            } else {
              log.info(`Cannot found user ${event.opid} to send wechat notification with result: ${JSON.stringify(presult)}`);
            }
          }
          break;
        }
        default: break;
      }
    })();
  });


  log.info(`wallet-trigger is running on ${process.env["PLAN-ORDER-EVENT-TRIGGER"]} and ${process.env["ADDITIONAL-ORDER-EVENT-TRIGGER"]}`);
}
