import { Server, ServerContext, ServerFunction, CmdPacket, Permission, waitingAsync, msgpack_decode_async, rpc } from "hive-service";
import { RedisClient, Multi } from "redis";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import { verify, uuidVerifier, stringVerifier, numberVerifier } from "hive-verify";
import * as bluebird from "bluebird";
import { Account, AccountEvent, TransactionEvent, Wallet } from "./wallet-define";

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

export const server = new Server();

const allowAll: Permission[] = [["mobile", true], ["admin", true]];
const mobileOnly: Permission[] = [["mobile", true], ["admin", false]];
const adminOnly: Permission[] = [["mobile", false], ["admin", true]];

server.callAsync("getWallet", allowAll, "获取钱包实体", "包含用户所有帐号", async (ctx: ServerContext, slim?: boolean, uid?: string) => {
  if (!uid) {
    uid = ctx.uid;
  }
  if (slim === undefined) {
    slim = true;
  }
  log.info(`getWallet, slim: ${slim}, uid: ${uid}`);
  try {
    verify([uuidVerifier("uid", ctx.uid)]);
  } catch (error) {
    ctx.report(3, error);
    return { code: 400, msg: "参数无法通过验证: " + error.message };
  }

  const buf = await ctx.cache.hgetAsync(slim ? "wallet-slim-entities" : "wallet-entities", uid);
  if (buf) {
    const wallet = await msgpack_decode_async(buf);
    return { code: 200, data: wallet };
  } else {
    return { code: 404, msg: "钱包不存在" };
  }
});

server.callAsync("getTransactions", allowAll, "获取交易记录", "获取钱包帐号下的交易记录", async (ctx: ServerContext, offset: number, limit: number, uid?: string) => {
  if (!uid) {
    uid = ctx.uid;
  }
  log.info(`getTransactions, offset: ${offset}, limit: ${limit}, uid: ${uid}`);
  try {
    verify([
      uuidVerifier("uid", uid),
      numberVerifier("offset", offset),
      numberVerifier("limit", limit)
    ]);
  } catch (error) {
    ctx.report(3, error);
    return { code: 400, msg: "参数无法通过验证: " + error.message };
  }
  const pkts = await ctx.cache.zrevrangebyscoreAsync(`transactions:${uid}`, offset, limit);
  const transactions = [];
  for (const pkt of pkts) {
    const transaction = await msgpack_decode_async(pkt);
    transactions.push(transaction);
  }
  return { code: 200, data: transactions };
});

server.callAsync("recharge", allowAll, "钱包充值", "来自order模块", async function (ctx: ServerContext, oid: string) {
  log.info(`recharge, oid: ${oid}, uid: ${ctx.uid}`);
  try {
    verify([uuidVerifier("uid", ctx.uid), uuidVerifier("oid", oid)]);
  } catch (error) {
    ctx.report(3, error);
    return { code: 400, msg: "参数无法通过验证: " + error.message };
  }

  const ordrep = await rpc(ctx.domain, process.env["ORDER"], ctx.uid, "getPlanOrder", oid);
  if (ordrep["code"] === 200) {
    const order = ordrep["data"];
    if (order.uid !== ctx.uid) {
      return { code: 404, msg: "不能对他人钱包充值！" };
    }

    const now = new Date();

    let aid = uuid.v4();
    const buf = await ctx.cache.hgetAsync("wallet-slim-entities", ctx.uid);
    if (buf) {
      const pkt = await msgpack_decode_async(buf);
      if (pkt) {
        const wallet: Wallet = pkt as Wallet;
        for (const account of (wallet.accounts || [])) {
          if (account.vid === order.vehicle.id) {
            aid = account.id; // replace aid with real aid
          }
        }
      }
    }

    const tevents: TransactionEvent[] = [
      {
        id:          uuid.v4(),
        type:        1,
        uid:         ctx.uid,
        title:       "加入计划充值",
        license:     order.vehicle.license,
        amount:      order.payment,
        occurred_at: new Date(now.getTime() + 1),
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
        undo:        false,
      },
      (order.summary != order.payment) ? {
        id:          uuid.v4(),
        type:        2,
        uid:         ctx.uid,
        title:       "优惠补贴",
        license:     order.vehicle.license,
        amount:      order.summary - order.payment,
        occurred_at: new Date(now.getTime() + 2),
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
        undo:        false,
      } :            null,
      {
        id:          uuid.v4(),
        type:        3,
        uid:         ctx.uid,
        title:       "缴纳管理费",
        license:     order.vehicle.license,
        amount:      -(order.summary * 0.2),
        occurred_at: new Date(now.getTime() + 3),
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
        undo:        false,
      },
      {
        id:          uuid.v4(),
        type:        4,
        uid:         ctx.uid,
        title:       "试运行期间管理费免缴，中途退出计划不可提现",
        license:     order.vehicle.license,
        amount:      (order.summary * 0.2),
        occurred_at: new Date(now.getTime() + 4),
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
        undo:        false,
      }
    ];

    const aevents: AccountEvent[] = [
      {
        id:          uuid.v4(),
        type:        3,
        opid:        ctx.uid,
        uid:         ctx.uid,
        occurred_at: new Date(now.getTime() + 3),
        amount:      order.summary * 0.2,
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
      },
      {
        id:          uuid.v4(),
        type:        5,
        opid:        ctx.uid,
        uid:         ctx.uid,
        occurred_at: new Date(now.getTime() + 5),
        amount:      order.summary * 0.8,
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
      }
    ];

    for (const event of aevents) {
      ctx.push("account-events", event);
    }

    const result = await waitingAsync(ctx);
    if (result["code"] === 200) {
      for (const event of tevents) {
        if (event) {
          ctx.push("transaction-events", event);
        }
      }
      return await waitingAsync(ctx);
    } else {
      return result;
    }
  } else {
    return { code: 404, msg: "订单不存在" };
  }
});

server.callAsync("freeze", adminOnly, "冻结资金", "用户账户产生资金冻结,账户余额不会改变", async (ctx: ServerContext, amount: number, maid: string, aid: string, type: number) => {
  log.info(`freeze, amount: ${amount}, maid: ${maid}, aid: ${aid}, type: ${type}`);
  try {
    verify([
      uuidVerifier("maid", maid),
      uuidVerifier("aid", aid),
      numberVerifier("amount", amount),
      numberVerifier("type", type)
    ]);
  } catch (error) {
    ctx.report(3, error);
    return { code: 400, msg: "参数无法通过验证: " + error.message };
  }
  if (type !== 0 && type !== 1) {
    return { code: 400, msg: "参数无法通过验证: type 必须为 0 或 1" };
  }

  const now = new Date();
  const tevent: TransactionEvent = {
    id:          uuid.v4(),
    type:        6,
    aid:         aid,
    title:       "互助金冻结",
    amount:      amount,
    occurred_at: now,
    maid:        maid,
    undo:        true,
  };
  ctx.push("transaction-events", tevent);
  const result = await waitingAsync(ctx);
  if (result["code"] === 200) {
    const aevent: AccountEvent = {
      id:          uuid.v4(),
      type:        0 ? 9: 11,
      opid:        ctx.uid,
      aid:         aid,
      occurred_at: now,
      amount:      amount,
      maid:        maid,
    };
    ctx.push("account-events", aevent);
    return await waitingAsync(ctx);
  } else {
    tevent.undo = true;
    ctx.push("transaction-events", tevent);
    return result;
  }
});

/*
server.call("unfreeze", adminOnly, "解冻资金", "用户账户资金解冻,账户余额不会改变", (ctx: ServerContext, rep: ((result: any) => void), amount: number, maid: string, aid: string) => {
  log.info(`unfreeze, amount: ${amount}, maid: ${maid}, aid: ${aid}`);
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
  const pkt: CmdPacket = { cmd: "debit", args: [domain, ctx.uid, amount, maid, cbflag] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, cbflag, rep);
});
*/

server.callAsync("replay", adminOnly, "重播事件", "重新执行所有已发生的事件", async (ctx: ServerContext, aid: string) => {
  log.info(`replay, aid: ${aid}`);
  const aevent: AccountEvent = {
    id:          null,
    type:        0,
    opid:        ctx.uid,
    aid:         aid,
    occurred_at: new Date(),
    amount:      0,
  };
  ctx.push("account-events", aevent);
  return await waitingAsync(ctx);
});

server.callAsync("refresh", adminOnly, "刷新", "刷新数据", async (ctx: ServerContext, uid?: string) => {
  if (uid) {
    log.info(`refresh, uid: ${uid}`);
  } else {
    log.info(`refresh`);
  }
  const pkt: CmdPacket = { cmd: "refresh", args: uid ? [uid] : [] };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

log.info("Start wallet server");
