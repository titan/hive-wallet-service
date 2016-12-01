import { Server, Config, Context, ResponseFunction, Permission, rpc, wait_for_response } from "hive-server";
import * as Redis from "redis";
import * as nanomsg from "nanomsg";
import * as msgpack from "msgpack-lite";
import * as bunyan from "bunyan";
import { servermap, triggermap } from "hive-hostmap";
import * as uuid from "node-uuid";
import { verify, uuidVerifier, stringVerifier } from "hive-verify";


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

let redis = Redis.createClient(6379, "redis"); // port, host

let wallet_entities = "wallet-entities";
let transactions = "transactions-";
let config: Config = {
  svraddr: servermap["wallet"],
  msgaddr: "ipc:///tmp/wallet.ipc",
  cacheaddr: process.env["CACHE_HOST"]
};

let svc = new Server(config);

let permissions: Permission[] = [["mobile", true], ["admin", true]];
// 来自order模块
svc.call("createAccount", permissions, (ctx: Context, rep: ResponseFunction, uid: string, type: string, vid: string, balance0: string, balance1: string) => {
  // let uid = ctx.uid;
  let aid = vid;
  let domain = ctx.domain;
  let args = { domain, uid, aid, type, vid, balance0, balance1 };
  log.info("createAccount", args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "createAccount", args: [domain, uid, aid, type, vid, balance0, balance1] }));
  rep({ status: "200", data: aid });
});

svc.call("getWallet", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("getwallet" + ctx.uid);
  if (!verify([uuidVerifier("uid", ctx.uid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  redis.hget(wallet_entities, ctx.uid, function (err, result) {
    if (err || result == "" || result == null) {
      log.info("get redis error in getwallet");
      log.info(err);
      rep({ code: 404, msg: "walletinfo not found for this uid" });
    } else {
      let sum = null;
      let accounts = JSON.parse(result);
      log.info(accounts);
      for (let account of accounts) {
        let balance = account.balance0 + account.balance1;
        sum += balance;
      }
      log.info("replies==========" + result);
      let result1 = { accounts: accounts, balance: sum, id: ctx.uid };
      rep({ code: 200, data: result1 });
    }
  });
});

svc.call("getTransactions", permissions, (ctx: Context, rep: ResponseFunction, offset: any, limit: any) => {
  log.info("getTransactions=====================");
  if (!verify([uuidVerifier("uid", ctx.uid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  redis.zrevrange(transactions + ctx.uid, offset, limit, function (err, result) {
    if (err || result === null || result == "") {
      log.info("get redis error in getTransactions");
      log.info(err);
      rep({ code: 500, msg: "未找到交易日志" });
    } else {
      // rep(JSON.parse(result));
      rep({ code: 200, data: result.map(e => JSON.parse(e)) });
    }
  });
});
//  来自order模块
svc.call("updateAccountbalance", permissions, (ctx: Context, rep: ResponseFunction, uid: string, vid: string, type1: string, balance0: string, balance1: string) => {
  log.info("getTransactions=====================");
  let domain = ctx.domain;
  let args = { domain, uid, vid, type1, balance0, balance1 };
  log.info("createAccount", args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "updateAccountbalance", args: [domain, uid, vid, type1, balance0, balance1] }));
  rep({ code: 200, data: "200" });
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

svc.call("agreeCashOut", permissions, (ctx: Context, rep: ResponseFunction, coid: string, state: number, user_id:string, opid:string) => {
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

console.log("Start service at " + config.svraddr);

svc.run();
