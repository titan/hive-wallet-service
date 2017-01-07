import { Server, ServerContext, ServerFunction, CmdPacket, Permission, wait_for_response, msgpack_decode } from "hive-service";
import { RedisClient, Multi } from "redis";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import { verify, uuidVerifier, stringVerifier, numberVerifier } from "hive-verify";
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


// 前端禁用
server.call("createAccount", adminOnly, "初始化钱包帐号", "初始化钱包帐号，若钱包不存在，则创建钱包", (ctx: ServerContext, rep: ((result: any) => void), vid: string, pid: string, uid?: string) => {
  log.info(`createAccount, vid: ${vid}, pid: ${pid}, uid: ${uid ? uid : ctx.uid}`);
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
  const pkt: CmdPacket = { cmd: "createAccount", args: [domain, uid ? uid : ctx.uid, vid, pid, aid, cbflag] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, cbflag, rep);
});

// 删除该接口，创建帐号的时候检查是否有钱包，没有就创建
// server.call("createWallet", adminOnly, "初始化钱包", "初始化钱包信息,提交订单时创建", (ctx: ServerContext, rep: (result: any) => void) => {
//   log.info(`createWallet, uid: ${ctx.uid}`);
//   if (!verify([uuidVerifier("uid", ctx.uid)], (errors: string[]) => {
//     rep({
//       code: 400,
//       msg: errors.join("\n")
//     });
//   })) {
//     return;
//   }
//   const wid = uuid.v1();
//   const domain = ctx.domain;
//   const cbflag = wid;
//   const pkt: CmdPacket = { cmd: "createWallet", args: [domain, ctx.uid, cbflag] };
//   ctx.publish(pkt);
//   wait_for_response(ctx.cache, cbflag, rep);
// });

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

// vid: , pid: , type0:, type1:, balance0: , balance1: , balance2: , title: string, oid: string, uid ?: string) => {
server.call("updateAccountBalance", adminOnly, "更新帐号余额", "唯一来源为订单充值", (ctx: ServerContext, rep: ((result) => void), vid: string, pid: string, type0: number, type1: number, balance0: number, balance1: number, balance2: number, title: string, oid: string, uid: string) => {
  log.info(`updateAccountBalance  domain:${ctx.domain},uid:${uid}, vid:${vid}, pid:${pid}, type0:${type0}, type1:${type1}, balance0:${balance0}, balance1:${balance1}, balance2:${balance2}, title:${title}, oid:${oid}`);
  const domain = ctx.domain;
  const cbflag = uuid.v1();
  (async () => {
    try {
      const aid = await ctx.cache.hgetAsync("vid-aid", vid + pid);
      if (aid === null || aid === "") {
        rep({
          code: 500,
          msg: "accounts not found"
        });
      } else {
        let args: Object[] = [];
        args = [domain, vid, aid, pid, type0, type1, balance0, balance1, balance2, title, oid, uid, cbflag];
        const pkt: CmdPacket = { cmd: "updateAccountBalance", args: args };
        ctx.publish(pkt);
        wait_for_response(ctx.cache, cbflag, rep);
      }
    } catch (e) {
      log.info(e);
      rep({
        code: 500,
        msg: e.message
      });
    }
  })();
});



server.call("recharge", allowAll, "钱包充值", "来自order模块", (ctx: ServerContext, rep: ((result: any) => void), oid: string) => {
  log.info(`recharge, oid: ${oid}, uid: ${ctx.uid}`);
  if (!verify([uuidVerifier("uid", ctx.uid), uuidVerifier("oid", oid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  const cbflag = uuid.v1();
  const domain = ctx.domain;
  const pkt: CmdPacket = { cmd: "recharge", args: [domain, ctx.uid, oid, cbflag] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, cbflag, rep);
});


server.call("freeze", adminOnly, "冻结资金", "用户账户产生资金冻结,账户余额不会改变", (ctx: ServerContext, rep: ((result: any) => void), amount: number, maid: string, aid: string, type: number) => {
  log.info(`freeze, amount: ${amount}, maid: ${maid},aid: ${aid}`);
  if (!verify([uuidVerifier("maid", maid), uuidVerifier("aid", aid), numberVerifier("amount", amount)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  const cbflag = uuid.v1();
  const domain = ctx.domain;
  const pkt: CmdPacket = { cmd: "freeze", args: [domain, ctx.uid, amount, maid, aid, type, cbflag] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, cbflag, rep);
});


server.call("unfreeze", adminOnly, "解冻资金", "用户账户资金解冻,账户余额不会改变", (ctx: ServerContext, rep: ((result: any) => void), amount: number, maid: string, aid: string) => {
  log.info(`unfreeze, amount: ${amount}, maid: ${maid},aid: ${aid}`);
  if (!verify([uuidVerifier("maid", maid), uuidVerifier("aid", aid), numberVerifier("amount", amount)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  const cbflag = uuid.v1();
  const domain = ctx.domain;
  const pkt: CmdPacket = { cmd: "unfreeze", args: [domain, ctx.uid, amount, maid, aid, cbflag] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, cbflag, rep);
});


server.call("debit", adminOnly, "扣款", "用户产生互助事件或者互助分摊扣款", (ctx: ServerContext, rep: ((result: any) => void), amount: number, maid: string) => {
  log.info(`debit, amount: ${amount}, maid: ${maid}`);
  if (!verify([uuidVerifier("maid", maid), numberVerifier("amount", amount)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  const cbflag = uuid.v1();
  const domain = ctx.domain;
  const pkt: CmdPacket = { cmd: "debit", args: [domain, ctx.uid, amount, maid, type, cbflag] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, cbflag, rep);
});


server.call("cashin", adminOnly, "增加提现金额", "用户计划到期或者提前退出计划", (ctx: ServerContext, rep: ((result: any) => void), amount: number, oid: string) => {
  log.info(`debit, amount: ${amount}, oid: ${oid}`);
  if (!verify([uuidVerifier("oid", oid), numberVerifier("amount", amount)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  const cbflag = uuid.v1();
  const domain = ctx.domain;
  const pkt: CmdPacket = { cmd: "debit", args: [domain, ctx.uid, amount, oid, cbflag] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, cbflag, rep);
});


server.call("cashout", adminOnly, "提用户现", "用户将可提现金额提现", (ctx: ServerContext, rep: ((result: any) => void), amount: number) => {
  log.info(`debit, amount: ${amount}`);
  if (!verify([numberVerifier("amount", amount)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  const cbflag = uuid.v1();
  const domain = ctx.domain;
  const pkt: CmdPacket = { cmd: "debit", args: [domain, ctx.uid, amount, cbflag] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, cbflag, rep);
});


server.call("refresh", adminOnly, "刷新", "刷新数据", (ctx: ServerContext, rep: ((result: any) => void)) => {
  const pkt: CmdPacket = { cmd: "refresh", args: null };
  ctx.publish(pkt);
  rep({ code: 200, data: "success" });
});

console.log("Start wallet server");
