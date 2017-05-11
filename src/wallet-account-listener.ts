import { BusinessEventContext, BusinessEventHandlerFunction, BusinessEventListener, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, waitingAsync, msgpack_decode_async, msgpack_encode_async, rpcAsync } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import * as bluebird from "bluebird";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import * as Disq from "hive-disque";
import { Vehicle } from "vehicle-library";
import { Account, Wallet } from "wallet-library";
import { AccountEvent } from "./wallet-define";

export const listener = new BusinessEventListener("account-events");

const log = bunyan.createLogger({
  name: "wallet-listener",
  streams: [
    {
      level: "info",
      path: "/var/log/wallet-account-listener-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/wallet-account-listener-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

function string_of_event_type(type: number) {
  switch (type) {
    case 0:  return "重播事件";
    case 1:  return "普通增加";
    case 2:  return "普通减少";
    case 3:  return "小池增加";
    case 4:  return "小池减少";
    case 5:  return "大池增加";
    case 6:  return "大池减少";
    case 7:  return "优惠增加";
    case 8:  return "优惠减少";
    case 9:  return "小池冻结";
    case 10: return "小池解冻";
    case 11: return "大池冻结";
    case 12: return "大池解冻";
    case 13: return "支付增加";
    case 14: return "支付减少";
    default: return "未知操作";
  }
}

function row2event(row): AccountEvent {
  let event = {
    id:          row.id,
    type:        row.type,
    opid:        row.opid,
    uid:         row.uid,
    aid:         row.aid,
    occurred_at: row.occurred_at,
    amount:      0.0,
    undo:        false,
  };

  if (row.data) {
    const data = row.data;
    event = data.oid ? { ...event, oid: data.oid } : event;
    event = data.vid ? { ...event, vid: data.vid } : event;
    event = data.maid ? { ...event, maid: data.maid } : event;
    event = data.amount ? { ...event, amount: data.amount } : event;
  }

  return event;
}

async function sync_account(db: PGClient, cache: RedisClient, account: Account) {
  if (account) {
    if (!account.vehicle) {
      const vrep = await rpcAsync<Vehicle>("mobile", process.env["VEHICLE"], account.uid, "getVehicle", account.vid);
      if (vrep.code === 200) {
        account.vehicle = vrep.data;
      }
    }
    const apkt = await msgpack_encode_async(account);
    await cache.hsetAsync("account-entities", account.id, apkt);
    let wallet = null;
    const wbuf = await cache.hgetAsync("wallet-entities", account.uid);
    if (wbuf) {
      wallet = await msgpack_decode_async(wbuf);
      if (wallet.accounts) {
        const accounts = [ account ];
        for (const a of wallet.accounts) {
          if (a.vid !== account.vid) {
            accounts.push(a);
          }
        }
        wallet.accounts = accounts;
      } else {
        wallet.accounts = [ account ];
      }
    } else {
      wallet = {
        frozen:   0.0,
        cashable: 0.0,
        balance:  0.0,
        accounts: [ account ],
      };
    }
    let frozen   = 0.0;
    let cashable = 0.0;
    let balance  = 0.0;
    for (const a of wallet.accounts) {
      frozen += a.frozen_balance0 + a.frozen_balance1;
      cashable += a.cashable_balance;
      balance += a.balance0 + a.balance1 + a.frozen_balance0 + a.frozen_balance1 + a.cashable_balance;
    }
    wallet.frozen   = frozen;
    wallet.cashable = cashable;
    wallet.balance  = balance;
    const wpkt = await msgpack_encode_async(wallet);
    await cache.hsetAsync("wallet-entities", account.uid, wpkt);
    for (const a of wallet.accounts) {
      const vehicle = {
        id: a.vehicle.id,
        license_no: a.vehicle.license_no,
      };
      a.vehicle = vehicle;
    }
    const wpkt2 = await msgpack_encode_async(wallet);
    await cache.hsetAsync("wallet-slim-entities", account.uid, wpkt2);
    return { code: 200, data: "Okay" };
  } else {
    return { code: 500, msg: "无法从事件流中合成帐号" };
  }
}

function play(account: Account, event: AccountEvent) {
  if (!account) {
    return null;
  }
  const newaccount: Account = { ... account, evtid: event.id, updated_at: event.occurred_at, uid: event.uid || account.uid || undefined, vid: event.vid || account.vid || undefined };
  switch (event.type) {
    case  1: return { ... newaccount, cashable_balance: account.cashable_balance + event.amount };
    case  2: return { ... newaccount, cashable_balance: account.cashable_balance - event.amount };
    case  3: return { ... newaccount, balance0: account.balance0 + event.amount };
    case  4: return { ... newaccount, balance0: account.balance0 - event.amount };
    case  5: return { ... newaccount, balance1: account.balance1 + event.amount };
    case  6: return { ... newaccount, balance1: account.balance1 - event.amount };
    case  7: return { ... newaccount, bonus: account.bonus + event.amount };
    case  8: return { ... newaccount, bonus: account.bonus - event.amount };
    case  9: return { ... newaccount, balance0: account.balance0 - event.amount, frozen_balance0: account.frozen_balance0 + event.amount };
    case 10: return { ... newaccount, balance0: account.balance0 + event.amount, frozen_balance0: account.frozen_balance0 - event.amount };
    case 11: return { ... newaccount, balance1: account.balance1 - event.amount, frozen_balance1: account.frozen_balance1 + event.amount };
    case 12: return { ... newaccount, balance1: account.balance1 + event.amount, frozen_balance1: account.frozen_balance1 - event.amount };
    case 13: return { ... newaccount, bonus: account.paid + event.amount };
    case 14: return { ... newaccount, bonus: account.paid - event.amount };
    default: return account;
  }
}

async function play_events(db: PGClient, cache: RedisClient, aid: string) {
  // 1. detect how many events haven't been played
  let since = null;
  let account: Account = null;
  const apkt = await cache.hgetAsync("account-entities", aid);
  if (apkt) {
    account = await msgpack_decode_async(apkt) as Account;
    const result0 = await db.query("SELECT occurred_at FROM account_events WHERE id = $1;", [account.evtid]);
    if (result0.rowCount !== 0) {
      since = result0.rows[0].occurred_at;
    } else {
      since = new Date(0);
    }
  } else {
    account = {
      id: aid,
      vid: null,
      uid: null,
      balance0: 0,
      balance1: 0,
      paid: 0,
      bonus: 0,
      frozen_balance0: 0,
      frozen_balance1: 0,
      cashable_balance: 0,
      evtid: null,
      created_at: new Date(),
      updated_at: new Date(),
    };
  }
  const eresult = await db.query("SELECT id, type, opid, uid, aid, occurred_at, data FROM account_events WHERE aid = $1 AND occurred_at > $2 AND deleted = false;", [aid, since]);
  if (eresult.rowCount > 0) {
    // 2. get the snapshot of the account
    for (const row of eresult.rows) {
      const event = row2event(row);
      account = play(account, event);
    }
    // 3. sync the snapshot of the account
    return await sync_account(db, cache, account);
  } else {
    // no event need to play
    return { code: 404, msg: "没有可以执行的事件" };
  }
}

async function handle_event(db: PGClient, cache: RedisClient, event: AccountEvent) {

  const eid         = event.id;
  const oid         = event.oid;
  const uid         = event.uid;
  const aid         = event.aid;
  const maid        = event.maid;
  const opid        = event.opid;
  const type        = event.type;
  const amount      = event.amount;
  const occurred_at = event.occurred_at;

  // whether has event occurred?
  // if not, save and play it
  const eresult = oid ?
    await db.query("SELECT id FROM account_events WHERE aid = $1 AND uid = $2 AND type = $3 AND data::jsonb->>'oid' = $4 AND deleted = false;", [aid, uid, type, oid]) :
    (maid ?
     await db.query("SELECT id FROM account_events WHERE aid = $1 AND uid = $2 AND type = $3 AND data::jsonb->>'maid' = $4 AND deleted = false;", [aid, uid, type, maid])
     :
       { rowCount: 0 }
    );
  if (eresult.rowCount === 0) {
    // event not found
    if (type !== 0) {
      await db.query("INSERT INTO account_events (id, type, opid, uid, aid, occurred_at, data) VALUES ($1, $2, $3, $4, $5, $6, $7);", [eid, type, opid, uid, aid, occurred_at, oid ? JSON.stringify({ oid , amount }) : JSON.stringify({ maid, amount })]);
    }
    const result = await play_events(db, cache, aid);
    return result;
  } else {
    return { code: 208, msg: "重复执行事件: " + string_of_event_type(type) };
  }
}

async function handle_undo_event(db: PGClient, cache: RedisClient, event: AccountEvent) {
  await db.query("DELETE FROM account_events WHERE id = $1;", [event.id]);
  return { code: 200, data: `AccountEvent ${event.id} deleted` };
}

listener.onEvent(async (ctx: BusinessEventContext, data: any) => {

  const event: AccountEvent = data as AccountEvent;
  const db: PGClient        = ctx.db;
  const cache: RedisClient  = ctx.cache;

  const type        = event.type;
  const oid         = event.oid;
  const aid         = event.aid;
  const maid        = event.maid;
  const opid        = event.opid;
  const vid         = event.vid;
  const amount      = event.amount;
  const occurred_at = event.occurred_at;
  let uid           = event.uid;

  log.info(`onEvent: id: ${event.id}, type: ${type}, oid: ${oid}, aid: ${aid}, maid: ${maid}, opid: ${opid}, vid: ${vid}, amount: ${amount}, occurred_at: ${occurred_at.toISOString()}, uid: ${uid}, undo: ${event.undo}`);

  if (!uid) {
    const result = await db.query("SELECT DISTINCT uid FROM account_events WHERE aid = $1;", [aid]);
    if (result.rowCount > 0) {
      uid = result.rows[0].uid;
      event.uid = uid;
    } else {
      return { code: 404, msg: "用户不存在，无法执行事件: " + string_of_event_type(type) };
    }
  }

  if (event.undo) {
    return handle_undo_event(db, cache, event);
  } else {
    return handle_event(db, cache, event);
  }
});
