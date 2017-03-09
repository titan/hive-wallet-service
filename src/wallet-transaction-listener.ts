import { BusinessEventContext, BusinessEventHandlerFunction, BusinessEventListener, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, waitingAsync, msgpack_decode_async, msgpack_encode_async } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import * as bluebird from "bluebird";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import * as Disq from "hive-disque";
import { Account, Transaction, TransactionEvent, Wallet } from "./wallet-define";

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

listener.onEvent(async function (ctx: BusinessEventContext, data: any) {

  const event              = data as TransactionEvent;
  const db: PGClient       = ctx.db;
  const cache: RedisClient = ctx.cache;

  let aid = event.aid;
  let uid = event.uid;

  if (!aid && !uid) {
    return { code: 404, msg: "需要 uid 或 aid" };
  }

  // get aid from database if it does not exist
  if (!aid) {
    if (!uid || !event.vid) {
      return { code: 404, msg: "需要提供 uid 和 vid" };
    }
    const result = await db.query("SELECT id FROM accounts WHERE uid = $1 AND vid = $2", [uid, event.vid]);
    if (result.rowCount === 1) {
      aid = result.rows[0].id;
    }
  }

  // get uid from database if it does not exist
  if (!uid) {
    const result = await db.query("SELECT DISTINCT uid FROM accounts WHERE id = $1", [uid]);
    if (result.rowCount === 1) {
      uid = result.rows[0].uid;
    }
  }

  // get license if it does not exist
  let license = event.license;
  if (!license) {
    const buf = await cache.hgetAsync("wallet-slim-entities", uid);
    if (buf) {
      const wallet: Wallet = (await msgpack_decode_async(buf)) as Wallet;
      if (wallet) {
        for (const account of wallet.accounts) {
          if (account.id === aid) {
            license = account.vehicle ? account.vehicle.license_no || null : null;
          }
        }
      }
    }
  }

  // check whether it is duplicate transaction
  const result = (event.type === 1 || event.type === 2 || event.type === 3 || event.type === 4) ?
    await db.query("SELECT 1 FROM transactions WHERE aid = $1 AND type = $2 AND data::jsonb->>'oid' = $3;", [aid, event.type, event.oid]) :
    ((event.type === 6 || event.type === 7) ?
     await db.query("SELECT 1 FROM transactions WHERE aid = $1 AND type = $2 AND data::jsonb->>'maid' = $3;", [aid, event.type, event.maid]) :
     await db.query("SELECT 1 FROM transactions WHERE aid = $1 AND type = $2 AND data::jsonb->>'sn' = $3;", [aid, event.type, event.sn]));

  if (result.rowCount === 0) {
    let data = {};

    data = event.sn ? { ...data, vid: event.sn } : data;
    data = event.oid ? { ...data, oid: event.oid } : data;
    data = event.vid ? { ...data, vid: event.vid } : data;
    data = event.maid ? { ...data, vid: event.maid } : data;

    // it's new transaction
    await db.query("INSERT INTO transactions (id, type, aid, uid, title, license, amount, data, occurred_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);", [event.id, event.type, aid, event.uid, event.title, license, event.amount, JSON.stringify(data), event.occurred_at]);
    const pkt = await msgpack_encode_async(event);
    await cache.zaddAsync(`transactions:${event.uid}`, event.occurred_at.getTime(), pkt);
    return { code: 200, data: "Okay" };
  } else {
    // it exists
    return { code: 208, msg: "重复的交易记录：" + (license ? "(" + license + ")" : "") + event.title };
  }
});
