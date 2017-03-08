import { BusinessEventContext, BusinessEventHandlerFunction, BusinessEventListener, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, waitingAsync, msgpack_decode_async, msgpack_encode_async } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import * as bluebird from "bluebird";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import * as Disq from "hive-disque";
import { Transaction, TransactionEvent } from "./wallet-define";

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

  // get aid from database if it does not exist
  let aid = null;
  if (event.type === 1 || event.type === 2 || event.type === 3 || event.type === 4) {
    const aresult = await db.query("SELECT id FROM accounts WHERE uid = $1 AND vid = $2", [event.uid, event.vid]);
    if (aresult.rowCount === 1) {
      aid = aresult.rows[0].id;
    }
  } else {
    aid = event.aid;
  }

  // check whether it is duplicate transaction
  const result = (event.type === 1 || event.type === 2 || event.type === 3 || event.type === 4) ?
    await db.query("SELECT 1 FROM transactions WHERE aid = $1 AND type = $2 AND data::jsonb->>oid = $3;", [aid, event.type, event.oid]) :
    ((event.type === 6 || event.type === 7) ?
     await db.query("SELECT 1 FROM transactions WHERE aid = $1 AND type = $2 AND data::jsonb->>maid = $3;", [aid, event.type, event.maid]) :
     await db.query("SELECT 1 FROM transactions WHERE aid = $1 AND type = $2 AND data::jsonb->>sn = $3;", [aid, event.type, event.sn]));

  if (result.rowCount === 0) {
    let data = {};

    data = event.sn ? { ...data, vid: event.sn } : data;
    data = event.oid ? { ...data, oid: event.oid } : data;
    data = event.vid ? { ...data, vid: event.vid } : data;
    data = event.maid ? { ...data, vid: event.maid } : data;

    // it's new transaction
    await db.query("INSERT INTO transactions (id, type, aid, uid, title, license, amount, data, occurred_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);", [event.id, event.type, aid, event.uid, event.title, event.license, event.amount, JSON.stringify(data), event.occurred_at]);
    const pkt = await msgpack_encode_async(event);
    await cache.zaddAsync(`transactions:${event.uid}`, event.occurred_at.getTime(), pkt);
    return { code: 200, data: "Okay" };
  } else {
    // it exists
    return { code: 208, msg: "重复的交易记录：" + (event.license ? "(" + event.license + ")" : "") + event.title };
  }
});
