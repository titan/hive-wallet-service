import { Server, ServerContext, ServerFunction, CmdPacket, Permission, waitingAsync, msgpack_decode_async, rpcAsync } from "hive-service";
import { RedisClient, Multi } from "redis";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import { verify, uuidVerifier, stringVerifier, numberVerifier } from "hive-verify";
import * as bluebird from "bluebird";
import { Account, Wallet, Transaction } from "wallet-library";
import { AccountEvent, TransactionEvent } from "./wallet-define";

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

server.callAsync("getWallet", allowAll, "获取钱包实体", "包含用户所有帐号", async (ctx: ServerContext, project: number = 1, slim?: boolean, uid?: string) => {
  if (!uid) {
    uid = ctx.uid;
  }
  if (slim === undefined) {
    slim = true;
  }
  log.info(`getWallet, slim: ${slim}, uid: ${uid}`);
  try {
    await verify([
      uuidVerifier("uid", ctx.uid),
      numberVerifier("project", project),
    ]);
  } catch (error) {
    ctx.report(3, error);
    return { code: 400, msg: error.message };
  }

  const buf = await ctx.cache.hgetAsync(slim ? `wallet-slim-entities-${project}` : `wallet-entities-${project}`, uid);
  if (buf) {
    const wallet = await msgpack_decode_async(buf) as Wallet;
    return { code: 200, data: convert_wallet_unit(wallet) };
  } else {
    return { code: 404, msg: "钱包不存在" };
  }
});

server.callAsync("getTransactions", allowAll, "获取交易记录", "获取钱包帐号下的交易记录", async (ctx: ServerContext, offset: number, limit: number, project: number = 1, uid?: string) => {
  if (!uid) {
    uid = ctx.uid;
  }
  log.info(`getTransactions, offset: ${offset}, limit: ${limit}, project: ${project}, uid: ${uid}`);
  try {
    await verify([
      uuidVerifier("uid", uid),
      numberVerifier("project", project),
      numberVerifier("offset", offset),
      numberVerifier("limit", limit),
    ]);
  } catch (error) {
    ctx.report(3, error);
    return { code: 400, msg: error.message };
  }
  const pkts = await ctx.cache.zrevrangeAsync(`transactions-${project}:${uid}`, offset, limit);
  const transactions = [];
  log.info("got transactions: " + pkts.length);
  for (const pkt of pkts) {
    const transaction = await msgpack_decode_async(pkt) as Transaction;
    transactions.push(convert_transaction_unit(transaction));
  }
  return { code: 200, data: transactions };
});

server.callAsync("rechargePlanOrder", allowAll, "钱包充值", "对计划订单钱包充值", async function (ctx: ServerContext, oid: string) {
  log.info(`rechargePlanOrder, oid: ${oid}, uid: ${ctx.uid}, sn: ${ctx.sn}`);
  try {
    await verify([uuidVerifier("uid", ctx.uid), uuidVerifier("oid", oid)]);
  } catch (error) {
    ctx.report(3, error);
    return { code: 400, msg: error.message };
  }
  const pkt: CmdPacket = { cmd: "rechargePlanOrder", args: [oid] };
  ctx.publish(pkt);
  const result = await waitingAsync(ctx);
  if (result.code !== 200) {
    const e: Error = new Error();
    e.name = result.code.toString();
    e.message = result.msg;
    ctx.report(0, e);
  }
  return result;
});

server.callAsync("rechargeThirdOrder", allowAll, "钱包充值", "对三者订单钱包充值", async function (ctx: ServerContext, oid: string) {
  log.info(`rechargeThirdOrder, oid: ${oid}, uid: ${ctx.uid}, sn: ${ctx.sn}`);
  try {
    await verify([uuidVerifier("uid", ctx.uid), uuidVerifier("oid", oid)]);
  } catch (error) {
    ctx.report(3, error);
    return { code: 400, msg: error.message };
  }
  const pkt: CmdPacket = { cmd: "rechargeThirdOrder", args: [oid] };
  ctx.publish(pkt);
  const result = await waitingAsync(ctx);
  if (result.code !== 200) {
    const e: Error = new Error();
    e.name = result.code.toString();
    e.message = result.msg;
    ctx.report(0, e);
  }
  return result;
});

server.callAsync("rechargeDeathOrder", allowAll, "钱包充值", "对死亡订单钱包充值", async function (ctx: ServerContext, oid: string) {
  log.info(`rechargeDeathOrder, oid: ${oid}, uid: ${ctx.uid}, sn: ${ctx.sn}`);
  try {
    await verify([uuidVerifier("uid", ctx.uid), uuidVerifier("oid", oid)]);
  } catch (error) {
    ctx.report(3, error);
    return { code: 400, msg: error.message };
  }
  const pkt: CmdPacket = { cmd: "rechargeDeathOrder", args: [oid] };
  ctx.publish(pkt);
  const result = await waitingAsync(ctx);
  if (result.code !== 200) {
    const e: Error = new Error();
    e.name = result.code.toString();
    e.message = result.msg;
    ctx.report(0, e);
  }
  return result;
});

server.callAsync("freeze", adminOnly, "冻结资金", "用户账户产生资金冻结,账户余额不会改变", async (ctx: ServerContext, aid: string, type: number, amount: number, maid: string) => {
  log.info(`freeze, aid: ${aid}, type: ${type}, amount: ${amount}, maid: ${maid}`);
  try {
    await verify([
      stringVerifier("maid", maid),
      uuidVerifier("aid", aid),
      numberVerifier("amount", amount),
      numberVerifier("type", type),
    ]);
  } catch (error) {
    ctx.report(3, error);
    return { code: 400, msg: error.message };
  }
  if (type !== 1 && type !== 2 && type !== 3) {
    ctx.report(3, new Error("参数无法通过验证: type 必须为 1, 2 或 3"));
    return { code: 400, msg: "参数无法通过验证: type 必须为 1, 2 或 3" };
  }
  const pkt: CmdPacket = { cmd: "freeze", args: [aid, type, Math.round(amount * 100), maid] };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("unfreeze", adminOnly, "解冻资金", "用户账户资金解冻,账户余额不会改变", async (ctx: ServerContext, amount: number, maid: string, aid: string, type: number) => {
  log.info(`unfreeze, amount: ${amount}, maid: ${maid}, aid: ${aid}`);
  try {
    await verify([
      uuidVerifier("maid", maid),
      uuidVerifier("aid", aid),
      numberVerifier("amount", amount),
      numberVerifier("type", type),
    ]);
  } catch (error) {
    ctx.report(3, error);
    return { code: 400, msg: error.message };
  }
  if (type !== 1 && type !== 2 && type !== 3) {
    return { code: 400, msg: "参数无法通过验证: type 必须为 1, 2 或 3" };
  }

  const converted_amount = Math.round(amount * 100);
  const now = new Date();
  const tevent: TransactionEvent = {
    id:          uuid.v4(),
    type:        7,
    aid:         aid,
    title:       "互助金解冻",
    amount:      converted_amount,
    occurred_at: now,
    maid:        maid,
    undo:        false,
    project:     1,
  };
  ctx.push("transaction-events", tevent);
  const result = await waitingAsync(ctx);
  if (result["code"] === 200) {
    const aevent: AccountEvent = {
      id:          uuid.v4(),
      type:        0 ? 10 : 12,
      opid:        ctx.uid,
      aid:         aid,
      occurred_at: now,
      amount:      converted_amount,
      maid:        maid,
      undo:        false,
      project:     1,
    };
    ctx.push("account-events", aevent);
    const result1 = await waitingAsync(ctx);
    if (result1["code"] === 200) {
      return result1;
    } else {
      tevent.undo = true;
      ctx.push("transaction-events", tevent);
      await waitingAsync(ctx);
      return result;
    }
  } else {
    return result;
  }
});

server.callAsync("deduct", adminOnly, "扣款", "用户产生互助事件或者互助分摊扣款", async (ctx: ServerContext, aid: string, amount: number, type: number, maid?: string, sn?: string) => {
  log.info(`deduct, aid: ${aid}, amount: ${amount}, type: ${type}, maid: ${maid}, sn: ${sn}`);
  try {
    await verify([
      uuidVerifier("aid", aid),
      numberVerifier("amount", amount),
      numberVerifier("type", type),
      maid ? stringVerifier("maid", maid) : undefined,
      sn ? stringVerifier("sn", sn) : undefined,
    ].filter(x => x));
  } catch (error) {
    ctx.report(3, error);
    return { code: 400, msg: error.message };
  }
  const pkt: CmdPacket = { cmd: "deduct", args: [aid, Math.round(amount * 100), type, maid, sn] };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("exportAccounts", adminOnly, "导出帐号信息", "导出所有帐号信息到指定的 csv 文件", async (ctx: ServerContext, filename: string) => {
  log.info(`exportAccounts, filename: ${filename}`);
  try {
    await verify([
      stringVerifier("filename", filename),
    ]);
  } catch (error) {
    ctx.report(3, error);
    return { code: 400, msg: error.message };
  }
  const pkt: CmdPacket = { cmd: "exportAccounts", args: [ filename ] };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("replay", adminOnly, "重播事件", "重新执行帐号下所有已发生的事件", async (ctx: ServerContext, aid: string) => {
  log.info(`replay, aid: ${aid}`);
  try {
    await verify([
      uuidVerifier("aid", aid),
    ]);
  } catch (error) {
    ctx.report(3, error);
    return { code: 400, msg: error.message };
  }
  let project = 1;
  const apkt = await ctx.cache.hgetAsync("account-entities", aid);
  if (apkt) {
    const account: Account = await msgpack_decode_async(apkt) as Account;
    if (account) {
      project = account.project;
    }
  }
  const aevent: AccountEvent = {
    id:          null,
    type:        0,
    opid:        ctx.uid,
    aid:         aid,
    occurred_at: new Date(),
    amount:      0,
    undo:        false,
    project:     project,
  };
  ctx.push("account-events", aevent);
  return await waitingAsync(ctx);
});

server.callAsync("replayAll", adminOnly, "重播事件", "重新执行所有帐号下所有已发生的事件", async (ctx: ServerContext) => {
  log.info(`replayAll`);
  const multi = bluebird.promisifyAll(ctx.cache.multi()) as Multi;
  multi.del("account-entities");
  multi.del("wallet-entities-1");
  multi.del("wallet-entities-2");
  multi.del("wallet-entities-3");
  multi.del("wallet-slim-entities-1");
  multi.del("wallet-slim-entities-2");
  multi.del("wallet-slim-entities-3");
  await multi.execAsync();
  const pkt: CmdPacket = { cmd: "replayAll", args: [] };
  ctx.publish(pkt);
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

// for outputing wallet to client
function convert_wallet_unit(wallet: Wallet): Wallet {
  wallet.balance  /= 100;
  wallet.frozen   /= 100;
  wallet.cashable /= 100;
  wallet.accounts = wallet.accounts.map(x => convert_account_unit(x));
  return wallet;
}

function convert_account_unit(account: Account): Account {
  account.balance0         /= 100;
  account.balance1         /= 100;
  account.paid             /= 100;
  account.bonus            /= 100;
  account.cashable_balance /= 100;
  account.frozen_balance0  /= 100;
  account.frozen_balance1  /= 100;
  return account;
}

// for outputing transaction to client
function convert_transaction_unit(transaction: Transaction): Transaction {
  transaction.amount /= 100;
  return transaction;
}

log.info("Start wallet server");
