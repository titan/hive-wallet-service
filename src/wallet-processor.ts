import { Processor, ProcessorFunction, ProcessorContext, rpcAsync, msgpack_encode_async, msgpack_decode_async, waitingAsync } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import * as crypto from "crypto";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import * as bluebird from "bluebird";
import { Account, AccountEvent, Transaction, TransactionEvent, Wallet } from "./wallet-define";

const log = bunyan.createLogger({
  name: "wallet-processor",
  streams: [
    {
      level: "info",
      path: "/var/log/wallet-processor-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/wallet-processor-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

export const processor = new Processor();
processor.callAsync("recharge", async (ctx: ProcessorContext, oid: string) => {
  log.info(`recharge, oid: ${oid}, uid: ${ctx.uid}, sn: ${ctx.sn}`);
  const ordrep = await rpcAsync(ctx.domain, process.env["ORDER"], ctx.uid, "getPlanOrder", oid);
  if (ordrep["code"] === 200) {
    const order = ordrep["data"];
    if (order.uid !== ctx.uid) {
      return { code: 404, msg: "不能对他人钱包充值！" };
    }
    const now = new Date();
    const sn = crypto.randomBytes(64).toString("base64");
    let aid = uuid.v4();
    const dbresult = await ctx.db.query("SELECT id FROM accounts WHERE uid = $1 AND vid = $2;", [ctx.uid, order.vehicle.id]);
    if (dbresult.rowCount > 0) {
      aid = dbresult.rows[0].id;
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
      (Math.abs(order.summary - order.payment) > 0.01) ? {
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
        amount:      -(Math.round(order.summary * 20) / 100),
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
        amount:      (Math.round(order.summary * 20) / 100),
        occurred_at: new Date(now.getTime() + 4),
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
        undo:        false,
      }
    ].filter(x => x);

    const smoney = Math.round(order.summary * 20) / 100;
    const aevents: AccountEvent[] = [
      {
        id:          uuid.v4(),
        type:        3,
        opid:        ctx.uid,
        uid:         ctx.uid,
        occurred_at: new Date(now.getTime() + 3),
        amount:      smoney,
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
        undo:        false,
      },
      {
        id:          uuid.v4(),
        type:        5,
        opid:        ctx.uid,
        uid:         ctx.uid,
        occurred_at: new Date(now.getTime() + 5),
        amount:      order.summary - smoney,
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
        undo:        false,
      }
    ];
    for (const event of aevents) {
      ctx.push("account-events", event, sn);
    }
    const result = await waitingAsync(ctx, sn);
    if (result["code"] === 200) {
      const tsn = crypto.randomBytes(64).toString("base64");
      for (const event of tevents) {
        if (event) {
          ctx.push("transaction-events", event, tsn);
        }
      }
      const result0 = await waitingAsync(ctx, tsn);
      if (result0["code"] === 200) {
        return result0;
      } else {
        // rollback
        for (const event of aevents) {
          event.undo = true;
          ctx.push("account-events", event);
        }
        // replay
        const aevent: AccountEvent = {
          id:          null,
          type:        0,
          opid:        ctx.uid,
          aid:         aid,
          occurred_at: new Date(),
          amount:      0,
          undo:        false,
        };
        ctx.push("account-events", aevent);
        return { code: 500, msg: "更新钱包交易记录失败" };
      }
    } else {
      return result;
    }
  } else {
    return { code: 404, msg: "订单不存在" };
  }
});

processor.callAsync("deduct", async (ctx: ProcessorContext, aid: string, amount: number, type: number, maid?: string, sn?: string) => {
  log.info(`deduct, aid: ${aid}, amount: ${amount}, type: ${type}, maid: ${maid}, sn: ${sn}`);
  const aevents: AccountEvent[] = [];
  const now = new Date();
  const aresult = await ctx.db.query("SELECT id, uid, balance0, balance1 FROM accounts WHERE aid = $1", [aid]);

  if (aresult.rowCount === 1) {
    const uid = aresult.rows[0].uid;
    const sb = aresult.rows[0].balance0;
    const bb = aresult.rows[0].balance1;
    if (type === 3) {
      if (sb + bb < amount) {
        return { code: 409, msg: "扣款金额超出钱包账户余额" };
      }
      aevents.push({
        id:          uuid.v4(),
        type:        4,
        opid:        ctx.uid,
        uid:         uid,
        occurred_at: new Date(now.getTime()),
        amount:      sb,
        aid:         aid,
        undo:        false,
      });
      aevents.push({
        id:          uuid.v4(),
        type:        6,
        opid:        ctx.uid,
        uid:         uid,
        occurred_at: new Date(now.getTime() + 1),
        amount:      bb,
        aid:         aid,
        undo:        false,
      });
    } else if (type === 2) {
      if (bb < amount) {
        return { code: 409, msg: "扣款金额超出钱包账户余额" };
      }
      aevents.push({
        id:          uuid.v4(),
        type:        6,
        opid:        ctx.uid,
        uid:         ctx.uid,
        occurred_at: new Date(now.getTime()),
        amount:      amount,
        aid:         aid,
        undo:        false,
      });
    } else if (type === 1) {
      if (sb < amount) {
        return { code: 409, msg: "扣款金额超出钱包账户余额" };
      }
      aevents.push({
        id:          uuid.v4(),
        type:        4,
        opid:        ctx.uid,
        uid:         uid,
        occurred_at: new Date(now.getTime()),
        amount:      amount,
        aid:         aid,
        undo:        false,
      });
    }
    const asn = crypto.randomBytes(64).toString("base64");
    for (const event of aevents) {
      ctx.push("account-events", event, asn);
    }
    const result = await waitingAsync(ctx, asn);
    if (result["code"] === 200) {
      let license = null;
      try {
        const vresult = await ctx.db.query("SELECT DISTINCT title FROM transactions WHERE aid = $1", [aid]);
        if (vresult.rowCount > 0 && vresult.rows.filter(x => x && x !== '').length > 0) {
          license = vresult.rows.filter(x => x && x !== '')[0];
        }
      } catch (e) {
        ctx.report(3, e);
      }
      const tevent = {
        id:          uuid.v4(),
        type:        7,
        uid:         uid,
        title:       "互助金结算",
        license:     license,
        amount:      amount,
        occurred_at: new Date(now.getTime()),
        sn:          sn,
        aid:         aid,
        undo:        false,
      };
      const tsn = crypto.randomBytes(64).toString("base64");
      ctx.push("transaction-events", tevent, tsn);
      const result0 = await waitingAsync(ctx, tsn);
      if (result0["code"] === 200) {
        return result0;
      } else {
        // rollback
        for (const event of aevents) {
          event.undo = true;
          ctx.push("account-events", event);
        }
        // replay
        const aevent: AccountEvent = {
          id:          null,
          type:        0,
          opid:        ctx.uid,
          aid:         aid,
          occurred_at: new Date(),
          amount:      0,
          undo:        false,
        };
        ctx.push("account-events", aevent);
        return { code: 500, msg: "更新钱包交易记录失败" };
      }
    } else {
      return result;
    }
  } else {
    return { code: 404, msg: "扣款钱包帐号不存在" };
  }
});

function refresh_accounts(db, cache, domain: string, uid?: string): Promise<void> {
  return sync_accounts(db, cache, domain, uid);
}

async function sync_accounts(db, cache, domain: string, uid?: string): Promise<void> {
  if (!uid) {
    await cache.delAsync("wallet-entities");
    await cache.delAsync("wallet-slim-entities");
  }
  const result = await db.query("SELECT id, vid, uid, balance0, balance1, bonus, frozen_balance0, frozen_balance1, cashable_balance, evtid, created_at, updated_at FROM accounts WHERE deleted = false " + (uid ? "AND uid = $1" : "ORDER BY uid"), uid ? [uid] : []);
  const wallets: Wallet[] = [];
  const accounts: Account[] = [];
  let wallet: Wallet = null;
  for (const row of result.rows) {
    if (!wallet) {
      wallet = {
        uid:      row.uid,
        frozen:   0.0,
        cashable: 0.0,
        balance:  0.0,
        accounts: []
      };
    }
    if (wallet.uid !== row.uid) {

      let frozen   = 0.0;
      let cashable = 0.0;
      let balance  = 0.0;

      for (const account of wallet.accounts) {
        frozen   += account.frozen_balance0;
        frozen   += account.frozen_balance1;
        cashable += account.cashable_balance;
        balance  += account.balance0;
        balance  += account.balance1;
      }
      balance += frozen + cashable;

      wallet.frozen   = frozen;
      wallet.cashable = cashable;
      wallet.balance  = balance;

      wallets.push(wallet);
      wallet = {
        uid:      row.uid,
        frozen:   0.0,
        cashable: 0.0,
        balance:  0.0,
        accounts: []
      };
    }
    const account: Account = {
      id:               row.id,
      uid:              row.uid,
      vid:              row.vid,
      balance0:         parseFloat(row.balance0),
      balance1:         parseFloat(row.balance1),
      bonus:            parseFloat(row.bonus),
      frozen_balance0:  parseFloat(row.frozen_balance0),
      frozen_balance1:  parseFloat(row.frozen_balance1),
      cashable_balance: parseFloat(row.cashable_balance),
      evtid:            row.evtid,
      created_at:       row.created_at,
      updated_at:       row.updated_at,
      vehicle:          null,
    };
    const vrep = await rpcAsync<Object>(domain, process.env["VEHICLE"], row.uid, "getVehicle", row.vid);
    if (vrep["code"] === 200) {
      account["vehicle"] = vrep["data"];
    }
    wallet.accounts.push(account);
  }
  if (wallet) {

    let frozen   = 0.0;
    let cashable = 0.0;
    let balance  = 0.0;

    for (const account of wallet.accounts) {
      frozen   += account.frozen_balance0;
      frozen   += account.frozen_balance1;
      cashable += account.cashable_balance;
      balance  += account.balance0;
      balance  += account.balance1;
    }
    balance += frozen + cashable;

    wallet.frozen   = frozen;
    wallet.cashable = cashable;
    wallet.balance  = balance;

    wallets.push(wallet);
  }
  const multi = bluebird.promisifyAll(cache.multi()) as Multi;
  for (const wallet of wallets) {
    const pkt = await msgpack_encode_async(wallet);
    multi.hset("wallet-entities", wallet.uid, pkt);
  }
  await multi.execAsync();
  const multi1 = bluebird.promisifyAll(cache.multi()) as Multi;
  for (const wallet of wallets) {
    for (const account of wallet.accounts) {
      if (account.vehicle) {
        const vehicle = {
          id: account.vid,
          license_no: account.vehicle.license_no,
        };
        account.vehicle = vehicle;
      }
    }
    const pkt = await msgpack_encode_async(wallet);
    multi1.hset("wallet-slim-entities", wallet.uid, pkt);
  }
  return multi1.execAsync();
}

function refresh_transactions(db, cache, domain: string, uid?: string): Promise<void> {
  return sync_transactions(db, cache, domain, uid);
}

async function sync_transactions(db, cache, domain: string, uid?: string): Promise<void> {
  if (!uid) {
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    const keys = await cache.keysAsync("transactions:*");
    for (const key of keys) {
      multi.del(key);
    }
    await multi.execAsync();
  }
  const result = await db.query("SELECT id, uid, aid, type, license, title, amount, occurred_at, data FROM transactions WHERE deleted = false" + (uid ? " AND uid = $1 ORDER BY occurred_at;" : " ORDER BY occurred_at;"), uid ? [uid] : []);
  const transactions: Transaction[] = [];
  for (const row of result.rows) {
    let transaction: Transaction = {
      id:          row.id,
      type:        row.type,
      uid:         row.uid,
      aid:         row.aid,
      license:     row.license,
      title:       row.title,
      amount:      parseFloat(row.amount),
      occurred_at: row.occurred_at,
    };
    if (row.data) {
      transaction = row.data.oid ? { ...transaction, oid: row.data.oid } : transaction;
      transaction = row.data.maid ? { ...transaction, maid: row.data.maid } : transaction;
      transaction = row.data.sn ? { ...transaction, sn: row.data.sn } : transaction;
    }
    transactions.push(transaction);
  }

  const multi1 = bluebird.promisifyAll(cache.multi()) as Multi;
  for (const transaction of transactions) {
    const pkt = await msgpack_encode_async(transaction);
    multi1.zadd("transactions:" + transaction.uid, transaction.occurred_at.getTime(), pkt);
  }
  return multi1.execAsync();
}

processor.callAsync("refresh", async (ctx: ProcessorContext, uid?: string) => {
  log.info("refresh" + (uid ? `, uid: ${uid}` : ""));
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const RW = await refresh_accounts(db, cache, ctx.domain, uid);
  const RT = await refresh_transactions(db, cache, ctx.domain, uid);
  return { code: 200, data: "Refresh done!" };
});

processor.callAsync("replayAll", async (ctx: ProcessorContext) => {
  log.info("replay");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const result = await db.query("SELECT id FROM accounts;");
  if (result.rowCount > 0) {
    for (const row of result.rows) {
      const aid = row.id;
      const event: AccountEvent = {
        id:          null,
        type:        0,
        opid:        ctx.uid,
        aid:         aid,
        occurred_at: null,
        amount:      0,
        undo:        false,
      };
      ctx.push("account-events", event);
    }
    return  { code: 200, data: "Okay" };
  } else {
    return { code: 404, msg: "No account to replay events" };
  }
});

log.info("Start wallet processor");

