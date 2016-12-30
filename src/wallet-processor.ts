import { Processor, ProcessorFunction, ProcessorContext, rpc, set_for_response, msgpack_encode } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import * as http from "http";
import * as queryString from "querystring";
import { RedisClient, Multi } from "redis";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import * as bluebird from "bluebird";

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
const wxhost = process.env["WX_ENV"] === "test" ? "dev.fengchaohuzhu.com" : "m.fengchaohuzhu.com";

function getLocalTime(nS) {
  return new Date(parseInt(nS) * 1000).toLocaleString().replace(/:\d{1,2}$/, " ");
}

function trim(str: string) {
  if (str) {
    return str.trim();
  } else {
    return null;
  }
}

processor.call("createAccount", (ctx: ProcessorContext, domain: string, uid: string, vid: string, pid: string, aid: string, cbflag: string) => {
  log.info(`createAccount, domain: ${domain}, uid: ${uid}, vid: ${vid}, pid: ${pid}, aid: ${aid}, cbflag: ${cbflag}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  const wid = uid;
  (async () => {
    try {
      const result = await db.query("SELECT 1 FROM wallets WHERE uid = $1", [uid]);
      if (result.rowCount === 0) {
        const evtid: string = uuid.v4();
        const evt_type: number = 0;
        const data: Object = {
          id: evtid,
          type: evt_type,
          uid: uid,
          occurred_at: new Date()
        };
        await db.query("BEGIN");
        await db.query("INSERT INTO wallet_events(id,type,uid,data) VALUES($1,$2,$3,$4)", [evtid, evt_type, uid, data]);
        await db.query("INSERT INTO wallets(id,uid,balance,evtid) VALUES($1,$2,$3,$4)", [wid, uid, 0, vid, evtid]);
        await db.query("INSERT INTO accounts(id,uid,vid,pid,balance0,balance1,balance2) VALUES($1,$2,$3,$4,$5,$6,$7)", [aid, uid, vid, pid, 0, 0, 0]);
        await db.query("COMMIT");
      } else {
        await db.query("INSERT INTO accounts(id,uid,vid,pid,balance0,balance1,balance2) VALUES($1,$2,$3,$4,$5,$6,$7)", [aid, uid, vid, pid, 0, 0, 0]);
      }
    } catch (e) {
      log.info(e);
      try {
        await db.query("ROLLBACK");
      } catch (e1) {
        log.error(e1);
      }
      await set_for_response(cache, cbflag, {
        code: 500,
        msg: e.message
      });
      done();
      return;
    }
    await set_for_response(cache, cbflag, {
      code: 200,
      data: { code: 200, data: aid }
    });
    await sync_wallets(db, cache, domain, wid);
    done();
  })();
});

//  args: [domain, uid, vid, aid, type, type1, balance0, balance1, order_id, callback] };
processor.call("updateAccountBalance", (ctx: ProcessorContext, domain: any, uid: string, vid: string, aid: string, type: number, type1: number, balance0: number, balance1: number, order_id: string, title: string, callback: string) => {
  log.info("updateOrderState");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  const created_at = new Date().getTime();
  const created_at1 = getLocalTime(created_at / 1000);
  const occurred_at = new Date();
  const balance = balance0 + balance1;
  const tid = uuid.v4();
  const evtid = uuid.v4();
  const data = {
    id: evtid,
    type: type1,
    uid: uid,
    occurred_at: occurred_at,
    amount: balance,
    oid: order_id,
    aid: aid
  };
  (async () => {
    try {
      await db.query("BEGIN");
      await db.query("UPDATE wallets SET balance = $1,evtid = $2 WHERE id = $3", [balance, evtid, uid]);
      await db.query("UPDATE accounts SET balance0 = balance0 + $1,balance1 = balance1 + $2 WHERE id = $3", [balance0, balance1, vid]);
      await db.query("INSERT INTO transactions(id,aid,type,title,amount) VALUES($1,$2,$3,$4,$5)", [tid, vid, type1, title, balance]);
      await db.query("INSERT INTO wallet_events(id,type,uid,data) VALUES($1,$2,$3,$4)", [evtid, type1, uid, data]);
      await db.query("COMMIT");
    } catch (e) {
      log.info(e);
      try {
        await db.query("ROLLBACK");
      } catch (e1) {
        log.error(e1);
      }
      cache.setex(callback, 30, JSON.stringify({
        code: 500,
        msg: e.message
      }), (err, result) => {
        done();
      });
      return;
    }
    try {
      const wallet_entities = await cache.hgetAsync("wallet-entities", uid);
      let accounts = JSON.parse(wallet_entities);
      log.info(accounts);
      for (let account of accounts) {
        if (account["id"] === aid) {
          let balance01 = account["balance0"];
          let balance11 = account["balance1"];
          let balance02 = balance01 + balance0;
          let balance12 = balance11 + balance1;
          account["balance0"] = balance02;
          account["balance1"] = balance12;
        }
      }
      const multi = cache.multi();
      const transactions = { amount: balance, occurred_at: created_at1, aid: aid, id: uid, title: title, type: type };
      multi.hset("wallet-entities", uid, JSON.stringify(accounts));
      multi.zadd("transactions-" + uid, created_at, JSON.stringify(transactions));
      await multi.execAsync();
      await cache.setexAsync(callback, 30, JSON.stringify({
        code: 200,
        data: { code: 200, aid: aid }
      }));
    } catch (e) {
      log.info(e);
      cache.setex(callback, 30, JSON.stringify({
        code: 500,
        msg: e.message
      }), (err, result) => {
        done();
      });
      return;
    }
  })();
});

function refresh_wallets(db, cache, domain: string): Promise<void> {
  return sync_wallets(db, cache, domain);
}

async function sync_wallets(db, cache, domain: string, wid?: string): Promise<void> {
  if (!wid) {
    await cache.del("wallet-entities");
  }
  const result = await db.query("SELECT w.id AS wid, w.uid, w.frozen, w.cashable, w.balance, a.id AS aid, a.pid, a.vid, a.balance0, a.balance1, a.balance2 FROM wallets AS w INNER JOIN accounts AS a ON w.uid = a.uid WHERE w.deleted = false AND a.deleted = false " + (wid ? "AND uid = $1 ORDER BY wid, w.uid" : "ORDER BY wid, w.uid"), wid ? [ wid ] : []);
  const wallets = [];
  const accounts = [];
  let wallet = null;
  for (const row of result.rows) {
    if (wallet && wallet.id !== row.wid) {
      wallets.push(wallet);
    } else {
      wallet = {
        id: row.wid,
        uid: row.uid,
        frozen: row.frozen,
        cashable: row.cashable,
        balance: row.balance,
        accounts: []
      };
    }
    const account = {
      balance0: parseFloat(row.balance0),
      balance1: parseFloat(row.balance1),
      balance2: parseFloat(row.balance2),
      id: row.id,
      uid: row.uid,
      pid: row.pid,
      vid: row.vid,
      vehicle: null
    };
    const vrep = await rpc<Object>(domain, process.env["VEHICLE"], row.uid, "getVehicle", row.vid);
    if (vrep["code"] === 200) {
      account["vehicle"] = vrep["data"];
    }
    wallet.accounts.push(account);
  }
  if (wallet) {
    wallets.push(wallet);
  }
  const multi = bluebird.promisifyAll(cache.multi()) as Multi;
  for (const wallet of wallets) {
    const pkt = await msgpack_encode(wallet);
    multi.hset("wallet-entities", wallet.id, pkt);
  }
  return multi.execAsync();
}

async function refresh_transitions(db, cache, domain: string): Promise<void> {
  return sync_transitions(db, cache, domain);
}

async function sync_transitions(db, cache, domain: string): Promise<void> {
  const keys = await cache.keys("transactions:*");
  const multi = bluebird.promisifyAll(cache.multi()) as Multi;
  for (const key of keys) {
    multi.del(key);
  }
  await multi.execAsync();
  const result = await db.query("SELECT aid, type, title, amount, occurred_at FROM transactions");
  const transactions = [];
  for (const row of result.rows) {
    const transaction = {
      id: null,
      amount: parseFloat(row.amount),
      occurred_at: row.occurred_at,
      aid: row.aid,
      title: trim(row.title),
      type: row.type
    };
    transactions.push(transaction);
  }

  for (const transaction of transactions) {
    const pkt = await msgpack_encode(transaction);
    multi.zadd("transactions:" + transaction["aid"], new Date(transaction["occurred_at"]), pkt);
  }
  return multi.execAsync();
}

processor.call("refresh", (ctx: ProcessorContext, domain: string) => {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  (async () => {
    const RW = await refresh_wallets(db, cache, domain);
    const RT = await refresh_transitions(db, cache, domain);
    log.info("refresh done!");
    done();
  })();
});

console.log("Start wallet processor");

