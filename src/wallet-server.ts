import { Server, ServerContext, ServerFunction, CmdPacket, Permission, wait_for_response, msgpack_decode } from "hive-service";
import { RedisClient, Multi } from "redis";;
import * as bunyan from "bunyan";
import * as uuid from "uuid";
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

export const server = new Server();

const allowAll: Permission[] = [["mobile", true], ["admin", true]];
const mobileOnly: Permission[] = [["mobile", true], ["admin", false]];
const adminOnly: Permission[] = [["mobile", false], ["admin", true]];

server.call("createAccount", allowAll, "初始化钱包帐号", "初始化钱包帐号，若钱包不存在，则创建钱包", (ctx: ServerContext, rep: ((result: any) => void), vid: string, pid: string, uid?: string) => {
  log.info(`createAccount, vid: ${vid}, pid: ${pid}, uid: ${uid ? uid : ctx.uid }`);
  if (uid) {
    if (!verify([uuidVerifier("uid", uid), uuidVerifier("vid", vid), uuidVerifier("pid", pid)], (errors: string[]) => {
      rep({
        code: 400,
        msg: errors.join("\n")
      });
    })) {
      return;
    }
  } else {
    if (!verify([uuidVerifier("uid", ctx.uid), uuidVerifier("vid", vid), uuidVerifier("pid", pid)], (errors: string[]) => {
      rep({
        code: 400,
        msg: errors.join("\n")
      });
    })) {
      return;
    }
  }
  const aid = uuid.v1();
  const domain = ctx.domain;
  const cbflag = aid;
  const pkt: CmdPacket = { cmd: "createAccount", args: [ domain, uid ? uid : ctx.uid, vid, pid, aid, cbflag ] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, cbflag, rep);
});

server.call("getWallet", allowAll, "获取钱包实体", "包含用户所有帐号", (ctx: ServerContext, rep: ((result) => void)) => {
  log.info("getwallet" + ctx.uid);
  if (!verify([uuidVerifier("uid", ctx.uid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget("wallet-entities", ctx.uid, function (err, result) {
    if (err || result === "" || result === null) {
      if (err) {
        log.error(err);
      }
      rep({ code: 404, msg: "Wallet not found" });
    } else {
      let sum = 0;
      msgpack_decode(result).then(wallet => {
        for (const account of wallet["accounts"]) {
          const balance = account.balance0 * 100 + account.balance1 * 100 + account.balance2 * 100;
          sum += balance;
        }

        const result1 = { accounts: wallet["accounts"], balance: sum / 100, id: ctx.uid };
        rep({ code: 200, data: result1 });
      }).catch(e => {
        rep({ code: 500, data: "Wallet in cache is invalid" });
      });
    }
  });
});

server.call("getTransactions", allowAll, "获取交易记录", "获取钱包帐号下的交易记录", (ctx: ServerContext, rep: ((result) => void), aid: string, offset: number, limit: number) => {
  log.info(`getTransactions, aid: ${aid}, offset: ${offset}, limit: ${limit}`);
  if (!verify([uuidVerifier("aid", aid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  (async () => {
    try {
      const pkts = await ctx.cache.zrevrangebyscoreAsync(`transactions:${aid}`, offset, limit);
      const transactions = [];
      for (const pkt of pkts) {
        const transaction = await msgpack_decode(pkt);
        transactions.push(transaction);
      }
      rep({ code: 200, data: transactions });
    } catch (e) {
      log.error(e);
      rep({ code: 500, msg: e.message });
    }
  })();
});

server.call("updateAccountBalance", allowAll, "更新帐号余额", "产生充值，扣款，提现等", (ctx: ServerContext, rep: ((result) => void), aid: string, vid: string, pid: string, type: number, type1: number, balance0: number, balance1: number, balance2: number, uid?: string) => {
  const domain = ctx.domain;
  const cbflag = uuid.v1();
  // uid: string, vid: string, type: number, type1: number, balance0: number, balance1: number, order_id: string)
  const args = { domain, uid, vid, type, type1, balance0, balance1, balance2 };
  (async () => {
    try {
      const aid = await ctx.cache.hgetAsync("vid-aid", vid);
      const pkt: CmdPacket = { cmd: "updateAccountBalance", args: [domain, uid, vid, aid, type, type1, balance0, balance1, balance2, cbflag] };
      ctx.publish(pkt);
      wait_for_response(ctx.cache, cbflag, rep);
    } catch (e) {
      log.info(e);
      rep({
        code: 500,
        msg: e.message
      });
    }
  })();
});

server.call("refresh", adminOnly, "刷新", "刷新数据", (ctx: ServerContext, rep: ((result: any) => void)) => {
  const pkt: CmdPacket = { cmd: "refresh", args: null };
  ctx.publish(pkt);
  rep({ code: 200, data: "success" });
});

console.log("Start wallet server");
