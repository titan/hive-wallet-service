import { Processor, ProcessorFunction, ProcessorContext, rpcAsync, msgpack_encode_async, msgpack_decode_async, waitingAsync, Result } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import * as crypto from "crypto";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import * as bluebird from "bluebird";
import { Account, Transaction, Wallet } from "wallet-library";
import { PlanOrder } from "order-library";
import { Vehicle } from "vehicle-library";
import { AccountEvent, TransactionEvent } from "./wallet-define";
import * as fs from "fs";

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
  const ordrep = await rpcAsync<PlanOrder>(ctx.domain, process.env["ORDER"], ctx.uid, "getPlanOrder", oid);
  if (ordrep.code === 200) {
    const order: PlanOrder = ordrep.data;
    if (order.uid !== ctx.uid) {
      return { code: 404, msg: "不能对他人钱包充值！" };
    }
    const now = new Date();
    const sn = crypto.randomBytes(64).toString("base64");
    let aid = uuid.v4();
    const dbresult = await ctx.db.query("SELECT DISTINCT aid FROM account_events WHERE uid = $1 AND data ->> 'vid' = $2;", [ctx.uid, order.vehicle.id]);
    if (dbresult.rowCount > 0) {
      aid = dbresult.rows[0].id;
    }
    const summary = Math.round(order.summary * 100);
    const payment = Math.round(order.payment * 100);
    const fee = (Math.round(summary * 0.2) < 1) ? 1 : Math.round(summary * 0.2);
    const wechat_fee = Math.ceil(payment * order.commission_ratio);
    const tevents: TransactionEvent[] = [
      {
        id:          uuid.v4(),
        type:        1,
        uid:         ctx.uid,
        title:       "加入计划充值",
        license:     order.vehicle.license_no,
        amount:      payment,
        occurred_at: new Date(now.getTime() + 1),
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
        undo:        false,
      },
      order.payment_method === 2 ? {
        id:          uuid.v4(),
        type:        9,
        uid:         ctx.uid,
        title:       "微信支付手续费",
        license:     order.vehicle.license_no,
        amount:      wechat_fee,
        occurred_at: new Date(now.getTime() + 2),
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
        undo:        false,
      } :            null,
      (summary !== payment) ? {
        id:          uuid.v4(),
        type:        2,
        uid:         ctx.uid,
        title:       "优惠补贴",
        license:     order.vehicle.license_no,
        amount:      summary - payment,
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
        license:     order.vehicle.license_no,
        amount:      fee,
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
        license:     order.vehicle.license_no,
        amount:      fee,
        occurred_at: new Date(now.getTime() + 4),
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
        undo:        false,
      }
    ].filter(x => x);
    let smoney = 0;
    let bmoney = 0;
    let bonus = (summary > payment) ? (summary - payment) : 0 ;
    if (order.payment_method === 2) {
      const total = summary - wechat_fee;
      smoney = Math.round(total * 0.2);
      bmoney = total - smoney;
    } else {
      smoney = Math.round(summary * 0.2);
      bmoney = summary - smoney;
    }
    const aevents: AccountEvent[] = [
      {
        id:          uuid.v4(),
        type:        3,
        opid:        ctx.uid,
        uid:         ctx.uid,
        occurred_at: new Date(now.getTime() + 5),
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
        occurred_at: new Date(now.getTime() + 10),
        amount:      bmoney,
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
        undo:        false,
      },
      {
        id:          uuid.v4(),
        type:        13,
        opid:        ctx.uid,
        uid:         ctx.uid,
        occurred_at: new Date(now.getTime() + 15),
        amount:      order.payment_method === 2 ? payment - wechat_fee : payment,
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
        undo:        false,
      },
      (bonus > 0) ? {
        id:          uuid.v4(),
        type:        7,
        opid:        ctx.uid,
        uid:         ctx.uid,
        occurred_at: new Date(now.getTime() + 20),
        amount:      bonus,
        vid:         order.vehicle.id,
        oid:         order.id,
        aid:         aid,
        undo:        false,
      } : null,
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
          uid:         ctx.uid,
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

processor.callAsync("freeze", async (ctx: ProcessorContext, aid: string, type: number, amount: number, maid: string) => {
  log.info(`freeze, aid: ${aid}, type: ${type}, amount: ${amount}, maid: ${maid}`);
  const uresult = await ctx.db.query("SELECT DISTINCT uid FROM account_events WHERE aid = $1", [aid]);
  if (uresult.rowCount > 0) {
    const buf = await ctx.cache.hgetAsync("wallet-entities", uresult.rows[0].uid);
    const pkt = await msgpack_decode_async(buf);
    const wallet = pkt as Wallet;
    const aevents: AccountEvent[] = [];
    for (const account of wallet.accounts) {
      log.info("freeze: account: " + JSON.stringify(account, null, 2));
      if (account.id === aid) {
        const now = new Date();
        switch (type) {
          case 1: {
            const unfrozen = account.balance0 - account.frozen_balance0;
            const evt: AccountEvent = {
              id:          uuid.v4(),
              type:        9,
              opid:        ctx.uid,
              aid:         aid,
              occurred_at: now,
              amount:      (amount > unfrozen ? unfrozen : amount),
              maid:        maid,
              undo:        false,
            }
            aevents.push(evt);
            break;
          }
          case 2: {
            const unfrozen = account.balance1 - account.frozen_balance1;
            const evt: AccountEvent = {
              id:          uuid.v4(),
              type:        11,
              opid:        ctx.uid,
              aid:         aid,
              occurred_at: now,
              amount:      (amount > unfrozen ? unfrozen : amount),
              maid:        maid,
              undo:        false,
            }
            aevents.push(evt);
            break;
          }
          default: {
            const unfrozen0 = account.balance0 - account.frozen_balance0;
            const unfrozen1 = account.balance1 - account.frozen_balance1;
            let rest = amount;
            if (rest < unfrozen0) {
              const evt: AccountEvent = {
                id:          uuid.v4(),
                type:        9,
                opid:        ctx.uid,
                aid:         aid,
                occurred_at: now,
                amount:      rest,
                maid:        maid,
                undo:        false,
              }
              aevents.push(evt);
            } else {
              const evt: AccountEvent = {
                id:          uuid.v4(),
                type:        9,
                opid:        ctx.uid,
                aid:         aid,
                occurred_at: now,
                amount:      unfrozen0,
                maid:        maid,
                undo:        false,
              }
              aevents.push(evt);
              rest -= unfrozen0;
              if (rest > 0) {
                const evt: AccountEvent = {
                  id:          uuid.v4(),
                  type:        11,
                  opid:        ctx.uid,
                  aid:         aid,
                  occurred_at: new Date(now.getTime() + 100),
                  amount:      (rest > unfrozen1 ? unfrozen1 : rest),
                  maid:        maid,
                  undo:        false,
                }
                aevents.push(evt);
              }
            }
            break;
          }
        }
        break;
      }
    }
    if (aevents.length === 0) {
      return { code: 404, msg: "扣款钱包帐号不存在" };
    }
    let total = 0;
    for (const evt of aevents) {
      total += evt.amount;
    }
    const tevent: TransactionEvent = {
      id:          uuid.v4(),
      type:        6,
      aid:         aid,
      title:       "互助金预提",
      amount:      total,
      occurred_at: aevents[0].occurred_at,
      maid:        maid,
      undo:        false,
    };
    let found_account_error = false;
    let aresult = null;
    for (const evt of aevents) {
      const asn = crypto.randomBytes(64).toString("base64");
      ctx.push("account-events", evt, asn);
      aresult = await waitingAsync(ctx, asn);
      if (aresult.code !== 200) {
        found_account_error = true;
        break;
      }
    }
    if (!found_account_error) {
        const tsn = crypto.randomBytes(64).toString("base64");
        ctx.push("transaction-events", tevent, tsn);
        const tresult = await waitingAsync(ctx, tsn);
        if (tresult.code === 200) {
          return { code: 200, data: "冻结成功" }
        } else {
          const undoevents = aevents.map(x => { x.undo = true; return x; }).reverse();
          for (const evt of undoevents) {
            ctx.push("account-events", evt);
          }
          return { code: 500, msg: "Push transaction event error: " + tresult.msg };
        }
    } else {
      const undoevents = aevents.map(x => { x.undo = true; return x; }).reverse();
      for (const evt of undoevents) {
        ctx.push("account-events", evt);
      }
      return { code: 500, msg: "Push account event error: " + aresult.msg };
    }
  } else {
    return { code: 404, msg: "扣款钱包帐号不存在" };
  }
});

processor.callAsync("deduct", async (ctx: ProcessorContext, aid: string, amount: number, type: number, maid?: string, sn?: string) => {
  log.info(`deduct, aid: ${aid}, amount: ${amount}, type: ${type}, maid: ${maid}, sn: ${sn}`);
  const aevents: AccountEvent[] = [];
  const now = new Date();
  const uresult = await ctx.db.query("SELECT DISTINCT uid FROM account_events WHERE aid = $1", [aid]);
  if (uresult.rowCount > 0) {
    const buf = await ctx.cache.hgetAsync("wallet-entities", uresult.rows[0].uid);
    const wallet = await msgpack_decode_async(buf) as Wallet;
    let account: Account = null;
    for (const a of wallet.accounts) {
      if (a.id === aid) {
        account = a;
        break;
      }
    }
    if (account) {
      const uid = account.uid;
      const sb = account.balance0;
      const bb = account.balance1;
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
      let to_deduct = amount;
      if (to_deduct > account.bonus + account.paid) {
        return { code: 409, msg: "扣款金额超出物理钱包账户余额" };
      }
      if (to_deduct <= account.bonus) {
        aevents.push({
          id:          uuid.v4(),
          type:        8,
          opid:        ctx.uid,
          uid:         uid,
          occurred_at: new Date(now.getTime() + 5),
          amount:      to_deduct,
          aid:         aid,
          undo:        false,
        });
      } else if (account.bonus > 0) {
        aevents.push({
          id:          uuid.v4(),
          type:        8,
          opid:        ctx.uid,
          uid:         uid,
          occurred_at: new Date(now.getTime() + 5),
          amount:      account.bonus,
          aid:         aid,
          undo:        false,
        });
        to_deduct -= account.bonus;
      }
      if (to_deduct > 1) {
        aevents.push({
          id:          uuid.v4(),
          type:        14,
          opid:        ctx.uid,
          uid:         uid,
          occurred_at: new Date(now.getTime() + 10),
          amount:      to_deduct,
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
    }
  }
  return { code: 404, msg: "扣款钱包帐号不存在" };
});

processor.callAsync("exportAccounts", async (ctx: ProcessorContext, filename: string) => {
  const walletpkts = await ctx.cache.hvalsAsync("wallet-slim-entities");
  const wallets: Wallet[] = (await Promise.all(walletpkts.map(x => msgpack_decode_async(x)))) as Wallet[];
  const accounts = wallets.reduce((acc, x) => acc.concat(x.accounts), []);
  const oids = {};
  const uids = [];
  for (const account of accounts) {
    const aresult = await ctx.db.query("SELECT distinct id, uid, data->>'oid' AS oid FROM account_events WHERE aid = $1 AND data ? 'oid' AND deleted = false", [account.id]);
    if (aresult.rowCount > 0) {
      const oid = aresult.rows[0].oid;
      const uid = aresult.rows[0].uid;
      uids.push(uid);
      oids[account.id] = oid;
    }
  }
  const oreps = await Promise.all(Object.keys(oids).map(x => oids[x]).map(oid => rpcAsync(ctx.domain, process.env["ORDER"], ctx.uid, "getPlanOrder", oid)));
  const orders = oreps.filter((x: Result<any>) => x.code === 200).map(x => x.data).reduce((acc, x) => { acc[x["id"]] = x; return acc; }, {});
  const users = {}
  for (let i = 0, count = Math.ceil(uids.length / 10); i < count; i ++) {
    const subids = uids.slice(i * 10, Math.min((i + 1) * 10, uids.length));
    const ureps = await rpcAsync(ctx.domain, process.env["PROFILE"], ctx.uid, "getUsers", [...subids]);
    if (ureps.code === 200) {
      for (const key of Object.keys(ureps.data)) {
        users[key] = ureps.data[key];
      }
    }
  }
  const data = [["PRNID", "保单编号", "起保日期", "终保日期", "保险金额", "个人互助金", "公共互助金"].join(",")].concat(accounts.filter(x => oids[x.id] && orders[oids[x.id]] && orders[oids[x.id]]["state"] === 4 && users[orders[oids[x.id]].uid]).map(x => {
    const order = orders[oids[x.id]];
    const user  = users[orders[oids[x.id]].uid];
    const account = x;
    return [user.pnrid, order.no, order.start_at.toISOString(), order.stop_at.toISOString(), order.summary, x.balance0 / 100, x.balance1 / 100].join(",");
  }));
  return await new Promise((resolve, reject) => {
    fs.writeFile(filename, "\ufeff" + data.join("\r\n"), function (err) {
      if (err) {
        reject(err);
      } else {
        resolve({ code: 200, data: "okay" });
      }
    });
  });
});

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
  } else {
    await cache.delAsync(`transactions:${uid}`);
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
  const RT = await refresh_transactions(db, cache, ctx.domain, uid);
  return { code: 200, data: "Refresh done!" };
});

processor.callAsync("replayAll", async (ctx: ProcessorContext) => {
  log.info("replayAll");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const result = await db.query("SELECT DISTINCT aid FROM account_events;");
  if (result.rowCount > 0) {
    for (const row of result.rows) {
      const aid = row.aid;
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

