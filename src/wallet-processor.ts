import { Processor, ProcessorFunction, ProcessorContext, rpc, msgpack_encode_async, msgpack_decode_async } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import * as bluebird from "bluebird";
import { Account, Transaction, Wallet } from "./wallet-define";

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

function trim(str: string) {
  if (str) {
    return str.trim();
  } else {
    return null;
  }
}

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
    if (wallet && wallet.uid !== row.uid) {

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
    } else {
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
    const vrep = await rpc<Object>(domain, process.env["VEHICLE"], row.uid, "getVehicle", row.vid);
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
  for (const wallet of wallets) {
    for (const account of wallet.accounts) {
      if (account.vehicle) {
        const vehicle = {
          id: account.vid,
          license_no: account.vehicle.license_no,
        }
        account.vehicle = vehicle;
      }
    }
    const pkt = await msgpack_encode_async(wallet);
    multi.hset("wallet-slim-entities", wallet.uid, pkt);
  }
  return multi.execAsync();
}

async function refresh_transitions(db, cache, domain: string, uid?: string): Promise<void> {
  return sync_transitions(db, cache, domain, uid);
}

async function sync_transitions(db, cache, domain: string, uid?: string): Promise<void> {
  const multi = bluebird.promisifyAll(cache.multi()) as Multi;
  if (!uid) {
    const keys = await cache.keysAsync("transactions:*");
    for (const key of keys) {
      multi.del(key);
    }
    await multi.execAsync();
  }
  const result = await db.query("SELECT id, uid, aid, type, license, title, amount, occurred_at, data FROM transactions" + (uid ? " WHERE uid = $1 ORDER BY occurred_at;" : " ORDER BY occurred_at;"), uid ? [uid] : []);
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

  for (const transaction of transactions) {
    const pkt = await msgpack_encode_async(transaction);
    multi.zadd("transactions:" + transaction["uid"], new Date(transaction["occurred_at"]), pkt);
  }
  return multi.execAsync();
}

processor.callAsync("refresh", async (ctx: ProcessorContext, uid?: string) => {
  log.info("refresh" + (uid ? `, uid: ${uid}` : ""));
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const RW = await refresh_accounts(db, cache, ctx.domain, uid);
  const RT = await refresh_transitions(db, cache, ctx.domain, uid);
  return { code: 200, data: "Refresh done!" }
});

log.info("Start wallet processor");

