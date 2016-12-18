import { Server, ServerContext, ServerFunction, CmdPacket, Permission, wait_for_response } from "hive-service";
import { RedisClient, Multi } from "redis";;
import * as nanomsg from "nanomsg";
import * as msgpack from "msgpack-lite";
import * as bunyan from "bunyan";
import { servermap, triggermap } from "hive-hostmap";
import * as uuid from "node-uuid";
import { verify, uuidVerifier, stringVerifier } from "hive-verify";
import * as bluebird from "bluebird";

let log = bunyan.createLogger({
  name: "wallet-server",
  streams: [
    {
      level: "info",
      path: "/var/log/wallet-server-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/wallet-server-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let wallet_entities = "wallet-entities";
let transactions = "transactions-";

declare module "redis" {
  export interface RedisClient extends NodeJS.EventEmitter {
    hgetAsync(key: string, field: string): Promise<any>;
    hincrbyAsync(key: string, field: string, value: number): Promise<any>;
    setexAsync(key: string, ttl: number, value: string): Promise<any>;
    zrevrangebyscoreAsync(key: string, start: number, stop: number): Promise<any>;
  }
  export interface Multi extends NodeJS.EventEmitter {
    execAsync(): Promise<any>;
  }
}


const svc = new Server();

const allowAll: Permission[] = [["mobile", true], ["admin", true]];
const mobileOnly: Permission[] = [["mobile", true], ["admin", false]];
const adminOnly: Permission[] = [["mobile", false], ["admin", true]];
// 来自order模块
svc.call("createAccount", allowAll, "初始化钱包帐号", "初始化钱包帐号", (ctx: ServerContext, rep: ((result: any) => void), uid: string, type: number, vid: string, order_id: string) => {
  let aid = vid;
  let domain = ctx.domain;
  let callback = uuid.v1();
  let args = { domain, uid, aid, type, vid, order_id };
  log.info("createAccount", args);
  const pkt: CmdPacket = { cmd: "createAccount", args: [domain, uid, aid, type, vid, order_id, callback] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, callback, rep);
});

svc.call("getWallet", allowAll, "获取钱包实体", "包含用户所有帐号", (ctx: ServerContext, rep: ((result) => void)) => {
  log.info("getwallet" + ctx.uid);
  if (!verify([uuidVerifier("uid", ctx.uid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget(wallet_entities, ctx.uid, function (err, result) {
    if (err || result === "" || result === null) {
      log.info("get redis error in getwallet");
      log.info(err);
      rep({ code: 404, msg: "walletinfo not found for this uid" });
    } else {
      let sum = null;
      let accounts = JSON.parse(result);
      log.info(accounts);
      for (let account of accounts) {
        let balance = account.balance0 * 100 + account.balance1 * 100;
        sum += balance;
      }
      log.info("replies==========" + result);
      let result1 = { accounts: accounts, balance: sum / 100, id: ctx.uid };
      rep({ code: 200, data: result1 });
    }
  });
});

svc.call("getTransactions", allowAll, "获取交易记录", "用户所有交易记录", (ctx: ServerContext, rep: ((result) => void), offset: any, limit: any) => {
  log.info("getTransactions=====================");
  if (!verify([uuidVerifier("uid", ctx.uid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.zrevrange(transactions + ctx.uid, offset, limit, function (err, result) {
    if (err || result === null || result == "") {
      log.info("get redis error in getTransactions");
      log.info(err);
      rep({ code: 500, msg: "未找到交易日志" });
    } else {
      rep({ code: 200, data: result.map(e => JSON.parse(e)) });
    }
  });
});
//  来自order模块
svc.call("updateAccountbalance", allowAll, "更新帐号信息", "产生充值，扣款，提现等", (ctx: ServerContext, rep: ((result) => void), uid: string, vid: string, type: number, type1: number, balance0: number, balance1: number, order_id: string, title: string) => {
  log.info("getTransactions=====================");
  let domain = ctx.domain;
  let callback = uuid.v1();
  // uid: string, vid: string, type: number, type1: number, balance0: number, balance1: number, order_id: string)
  let args = { domain, uid, vid, type, type1, balance0, balance1, order_id };
  log.info("updateAccountbalance", args);
  (async () => {
    try {
      const aid = await ctx.cache.hgetAsync("vid-aid", vid);
      const pkt: CmdPacket = { cmd: "updateAccountbalance", args: [domain, uid, vid, aid, type, type1, balance0, balance1, order_id, title, callback] };
      ctx.publish(pkt);
      wait_for_response(ctx.cache, callback, rep);
    } catch (e) {
      log.info(e);
      rep({
        code: 500,
        msg: e
      });
    }
  })();
  ctx.msgqueue.send(msgpack.encode({ cmd: "updateAccountbalance", args: [domain, uid, vid, type1, balance0, balance1] }));

});

// svc.call("createFreezeAmountLogs", permissions, (ctx: Context, rep: ResponseFunction) => {
//   log.info("getTransactions=====================");
//   let domain = ctx.domain;
//   // let args = { domain, uid, vid, type1, balance0, balance1 };
//   // log.info("createAccount", args);
//   // ctx.msgqueue.send(msgpack.encode({ cmd: "updateAccountbalance", args: [domain, uid, vid, type1, balance0, balance1] }));
//   rep({ code: 200, status: "200" });
// });
svc.call("refresh", permissions, (ctx: Context, rep: ResponseFunction) => {
  ctx.msgqueue.send(msgpack.encode({ cmd: "refresh", args: [] }));
  rep({ code: 200, data: "200" });
});

svc.call("applyCashOut", permissions, (ctx: Context, rep: ResponseFunction, order_id: string) => {
  log.info("applyCashOut uuid is " + ctx.uid);
  let user_id = ctx.uid;
  if (!verify([uuidVerifier("order_id", order_id), uuidVerifier("user_id", user_id)], (errors: string[]) => {
    log.info(errors);
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let callback = uuid.v1();
  let domain = ctx.domain;
  ctx.msgqueue.send(msgpack.encode({ cmd: "applyCashOut", args: [domain, order_id, user_id, callback] }));
  wait_for_response(ctx.cache, callback, rep);
});

svc.call("agreeCashOut", permissions, (ctx: Context, rep: ResponseFunction, coid: string, state: number, user_id: string, opid: string) => {
  log.info("agreeCashOut uuid is " + ctx.uid);
  if (!verify([uuidVerifier("coid", coid), uuidVerifier("user_id", user_id)], (errors: string[]) => {
    log.info(errors);
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let callback = uuid.v1();
  let domain = ctx.domain;
  ctx.msgqueue.send(msgpack.encode({ cmd: "agreeCashOut", args: [domain, coid, state, opid, user_id, callback] }));
  wait_for_response(ctx.cache, callback, rep);
});

declare module "redis" {
  export interface RedisClient extends NodeJS.EventEmitter {
    hgetAsync(key: string, field: string): Promise<any>;
    zrevrangeAsync(key: string, field: number, field2: number): Promise<any>;
  }
  export interface Multi extends NodeJS.EventEmitter {
    execAsync(): Promise<any>;
  }
}

svc.call("getAppliedCashouts", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("getAppliedCashouts");
  (async () => {
    try {
      const keys: Object[] = await ctx.cache.zrevrangeAsync("applied-cashouts", 0, -1);
      let multi = bluebird.promisifyAll(ctx.cache.multi()) as Multi;
      for (let key of keys) {
        multi.hget("cashout-entities", key);
      }
      const cashouts = await multi.execAsync();
      rep({ code: 200, data: cashouts.map(e => JSON.parse(e)) });
    } catch (e) {
      log.error(e);
      rep({ code: 500, msg: e.message });
    }
  })();
});

svc.call("getAppliedCashouts", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("getAppliedCashouts");
  (async () => {
    try {
      const keys: Object[] = await ctx.cache.zrevrangeAsync("applied-cashouts", 0, -1);
      let multi = bluebird.promisifyAll(ctx.cache.multi()) as Multi;
      for (let key of keys) {
        multi.hget("cashout-entities", key);
      }
      const cashouts = await multi.execAsync();
      rep({ code: 200, data: cashouts.map(e => JSON.parse(e)) });
    } catch (e) {
      log.error(e);
      rep({ code: 500, msg: e.message });
    }
  })();
});

svc.call("getAgreedCashouts", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("getAgreedCashouts");
  (async () => {
    try {
      const keys: Object[] = await ctx.cache.zrevrangeAsync("agreed-cashouts", 0, -1);
      let multi = bluebird.promisifyAll(ctx.cache.multi()) as Multi;
      for (let key of keys) {
        multi.hget("cashout-entities", key);
      }
      const cashouts = await multi.execAsync();
      rep({ code: 200, data: cashouts.map(e => JSON.parse(e)) });
    } catch (e) {
      log.error(e);
      rep({ code: 500, msg: e.message });
    }
  })();
});

svc.call("getRefusedCashouts", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("getRefusedCashouts");
  (async () => {
    try {
      const keys: Object[] = await ctx.cache.zrevrangeAsync("refused-cashouts", 0, -1);
      let multi = bluebird.promisifyAll(ctx.cache.multi()) as Multi;
      for (let key of keys) {
        multi.hget("cashout-entities", key);
      }
      const cashouts = await multi.execAsync();
      rep({ code: 200, data: cashouts.map(e => JSON.parse(e)) });
    } catch (e) {
      log.error(e);
      rep({ code: 500, msg: e.message });
    }
  })();
});

console.log("Start service at " + config.svraddr);

svc.run();
