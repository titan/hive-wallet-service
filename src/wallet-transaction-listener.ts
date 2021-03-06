import { BusinessEventContext, BusinessEventHandlerFunction, BusinessEventListener, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, waitingAsync, msgpack_decode_async, msgpack_encode_async } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import * as bluebird from "bluebird";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import * as Disq from "hive-disque";
import { Account, Transaction, Wallet } from "wallet-library";
import { TransactionEvent } from "./wallet-define";

export const listener = new BusinessEventListener("transaction-events");

const log = bunyan.createLogger({
  name: "wallet-listener",
  streams: [
    {
      level: "info",
      path: "/var/log/wallet-transaction-listener-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/wallet-transaction-listener-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

async function handle_undo_event(db: PGClient, cache: RedisClient, event: TransactionEvent) {
  await db.query("DELETE FROM transactions WHERE id = $1;", [event.id]);
  await cache.zremrangebyscoreAsync(`transactions-${event.project}:${event.uid}`, event.occurred_at.getTime(), event.occurred_at.getTime());

  return { code: 200, data: `Transaction ${event.id} deleted` };
}

async function handle_event(db: PGClient, cache: RedisClient, event: TransactionEvent) {
  const aid = event.aid;
  const uid = event.uid;
  const project = event.project;
  // get license if it does not exist
  let license = event.license;
  if (!license) {
    const buf = await cache.hgetAsync(`wallet-slim-entities-${project}`, uid);
    if (buf) {
      const wallet: Wallet = (await msgpack_decode_async(buf)) as Wallet;
      if (wallet) {
        for (const account of wallet.accounts) {
          if (account.id === aid) {
            license = account.vehicle ? account.vehicle.license_no || null : null;
            break;
          }
        }
      }
    }
  }

  // check whether it is duplicate transaction
  let result = null;
  if (project === 1) {
    if (event.type === 1 || event.type === 2 || event.type === 3 || event.type === 4) {
      result = await db.query("SELECT 1 FROM transactions WHERE aid = $1 AND type = $2 AND data::jsonb->>'oid' = $3 AND project = $4 AND deleted = false;", [aid, event.type, event.oid, project]);
    } else {
      if (event.type === 6 || event.type === 7) {
        result = await db.query("SELECT 1 FROM transactions WHERE aid = $1 AND type = $2 AND data::jsonb->>'maid' = $3 AND data::jsonb->>'sn' = $4 AND project = $5 AND deleted = false;", [aid, event.type, event.maid, event.sn, project]);
      } else {
        result = await db.query("SELECT 1 FROM transactions WHERE aid = $1 AND type = $2 AND data::jsonb->>'sn' = $3 AND project = $4 AND deleted = false;", [aid, event.type, event.sn, project]);
      }
    }
  } else {
    result = await db.query("SELECT 1 FROM transactions WHERE aid = $1 AND type = $2 AND data::jsonb->>'oid' = $3 AND project = $4 AND deleted = false;", [aid, event.type, event.oid, project]);
  }

  if (result.rowCount === 0) {
    let data = {};

    data = event.sn ? { ...data, sn: event.sn } : data;
    data = event.oid ? { ...data, oid: event.oid } : data;
    data = event.vid ? { ...data, vid: event.vid } : data;
    data = event.maid ? { ...data, maid: event.maid } : data;

    // it's new transaction
    await db.query("INSERT INTO transactions (id, type, aid, uid, title, license, amount, data, occurred_at, project) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);", [event.id, event.type, aid, uid, event.title, license, event.amount, JSON.stringify(data), event.occurred_at, project]);
    const tresult = await db.query("SELECT id, type, aid, uid, title, license, amount, data, occurred_at FROM transactions WHERE uid = $1 AND project = $2 AND deleted = false ORDER BY occurred_at", [uid, project]);
    if (tresult.rowCount > 0) {
      const keys = await cache.keysAsync(`transactions-${project}:${uid}:*`);
      const multi = bluebird.promisifyAll(cache.multi()) as Multi;
      const key = `transactions-${project}:${uid}`;
      for (const k of keys) {
        multi.del(k);
      }
      multi.del(key);
      for (const row of tresult.rows) {
        const transaction: Transaction = {
          id: row.id,
          type: row.type,
          aid: row.aid,
          uid: row.uid,
          title: row.title,
          license: row.license,
          amount: row.amount,
          occurred_at: row.occurred_at,
          oid: row.data.oid || undefined,
          maid: row.data.maid || undefined,
          sn: row.data.sn || undefined,
          project: row.project,
        }
        const pkt = await msgpack_encode_async(transaction);
        multi.zadd(key, row.occurred_at.getTime(), pkt);
        multi.zadd(`transactions-${project}:${uid}:${row.license}`, row.occurred_at.getTime(), pkt);
      }
      await multi.execAsync();
      return { code: 200, data: "Okay" };
    } else {
      const pkt = await msgpack_encode_async(event);
      const multi = bluebird.promisifyAll(cache.multi()) as Multi;
      multi.zadd(`transactions-${project}:${uid}`, event.occurred_at.getTime(), pkt);
      multi.zadd(`transactions-${project}:${uid}:${event.license}`, event.occurred_at.getTime(), pkt);
      multi.execAsync();
      return { code: 200, data: "Okay" };
    }
  } else {
    // it exists
    return { code: 208, msg: "重复的交易记录：" + (license ? "(" + license + ")" : "") + event.title };
  }
}

listener.onEvent(async function (ctx: BusinessEventContext, data: any) {

  const event              = data as TransactionEvent;
  const db: PGClient       = ctx.db;
  const cache: RedisClient = ctx.cache;

  let aid = event.aid;
  let uid = event.uid;

  log.info(`onEvent: id: ${event.id}, type: ${event.type}, aid: ${aid}, uid: ${uid}, undo: ${event.undo}`);

  if (!aid && !uid) {
    return { code: 404, msg: "需要 uid 或 aid" };
  }

  // get aid from database if it does not exist
  if (!aid && event.project === 1) {
    if (!uid || !event.vid) {
      return { code: 404, msg: "需要提供 uid 和 vid" };
    }
    const result = await db.query("SELECT DISTINCT aid FROM account_events WHERE uid = $1 AND vid = $2", [uid, event.vid]);
    if (result.rowCount === 1) {
      aid = result.rows[0].id;
      event.aid = aid;
    }
  }

  // get uid from database if it does not exist
  if (!uid) {
    const result = await db.query("SELECT DISTINCT uid FROM account_events WHERE aid = $1", [aid]);
    if (result.rowCount === 1) {
      uid = result.rows[0].uid;
      event.uid = uid;
    }
  }

  if (event.undo) {
    return handle_undo_event(db, cache, event);
  } else {
    return handle_event(db, cache, event);
  }
});
