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
    amount:      0,
    undo:        false,
    project:     row.project,
  };

  if (row.data) {
    const data = row.data;
    event = data.oid ? { ...event, oid: data.oid } : event;
    event = data.vid ? { ...event, vid: data.vid } : event;
    event = data.maid ? { ...event, maid: data.maid } : event;
    event = data.amount ? { ...event, amount: data.amount } : event;
    event = data.license ? { ...event, license: data.license } : event;
    event = data.owner ? { ...event, owner: data.owner } : event;
  }

  return event;
}

async function sync_wallet(db: PGClient, cache: RedisClient, uid: string, project: number, account?: Account) {
  let wallet: Wallet = null;
  const accounts = account ? [ account ] : [];
  const wbuf = await cache.hgetAsync(`wallet-entities-${project}`, uid);
  if (wbuf) {
    wallet = await msgpack_decode_async(wbuf) as Wallet;
    if (wallet.accounts) {
      if (account) {
        for (const a of wallet.accounts) {
          if (a.id !== account.id) {
            accounts.push(a);
          } else {
            if (a.vehicle) {
              account.vehicle = a.vehicle;
            }
          }
        }
      } else {
        for (const a of wallet.accounts) {
          const pkt = await cache.hgetAsync("account-entities", a.id);
          if (pkt) {
            accounts.push(a);
          }
        }
      }
    }
    wallet.accounts = accounts;
  } else {
    wallet = {
      frozen:   0,
      cashable: 0,
      balance:  0,
      accounts: accounts,
      project: project,
    };
  }
  let frozen   = 0;
  let cashable = 0;
  let balance  = 0;
  for (const a of wallet.accounts) {
    frozen += a.frozen_balance0 + a.frozen_balance1;
    cashable += a.cashable_balance;
    balance += a.balance0 + a.balance1 + a.frozen_balance0 + a.frozen_balance1 + a.cashable_balance;
  }
  wallet.frozen   = frozen;
  wallet.cashable = cashable;
  wallet.balance  = balance;
  const wpkt = await msgpack_encode_async(wallet);
  await cache.hsetAsync(`wallet-entities-${project}`, uid, wpkt);
  for (const a of wallet.accounts) {
    if (a.vehicle) {
      const vehicle = {
        id: a.vehicle.id,
        license_no: a.vehicle.license_no,
      };
      a.vehicle = vehicle;
    }
  }
  const wpkt2 = await msgpack_encode_async(wallet);
  await cache.hsetAsync(`wallet-slim-entities-${project}`, uid, wpkt2);
  return { code: 200, data: wallet };
}

async function sync_account(db: PGClient, cache: RedisClient, account: Account) {
  if (account) {
    if (!account.vehicle && account.vid) {
      const vrep = await rpcAsync<Vehicle>("mobile", process.env["VEHICLE"], account.uid, "getVehicle", account.vid);
      if (vrep.code === 200) {
        account.vehicle = vrep.data;
      }
    }
    const apkt = await msgpack_encode_async(account);
    const multi: Multi = bluebird.promisifyAll(cache.multi()) as Multi;
    multi.hset("account-entities", account.id, apkt);
    if (account.project === 2 || account.project === 3) {
      multi.zadd(`accounts-${account.project}`, account.created_at.getTime(), account.id);
      if (account.owner && account.owner.phone) {
        multi.zadd(`accounts-of-phone-${account.project}:${account.owner.phone}`, account.created_at.getTime(), account.id);
      }
      if (account.license) {
        multi.zadd(`accounts-of-license-${account.project}:${account.license}`, account.created_at.getTime(), account.id);
      }
    }
    await multi.execAsync();
    return await sync_wallet(db, cache, account.uid, account.project, account);
  } else {
    return { code: 500, msg: "无法从事件流中合成帐号" };
  }
}

function play(account: Account, event: AccountEvent) {
  if (!account) {
    return null;
  }
  const newaccount: Account = { ... account, evtid: event.id, updated_at: event.occurred_at, uid: event.uid || account.uid || undefined, vid: account.vid || event.vid || undefined };
  if (!newaccount.created_at) {
    newaccount.created_at = event.occurred_at;
  }
  if (event.project === 2 || event.project === 3) {
    newaccount.license = event.license || account.license || undefined;
    newaccount.owner = event.owner || account.owner || undefined;
  }
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
  case 13: return { ... newaccount, paid: account.paid + event.amount };
  case 14: return { ... newaccount, paid: account.paid - event.amount };
  default: return account;
  }
}

async function play_events(db: PGClient, cache: RedisClient, aid: string, project: number) {
  // 1. detect how many events haven't been played
  let since = null;
  let account: Account = null;
  const apkt = await cache.hgetAsync("account-entities", aid);
  if (apkt) {
    account = await msgpack_decode_async(apkt) as Account;
    since = account.updated_at;
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
      created_at: null,
      updated_at: null,
      project: project,
    };
    since = new Date(0);
  }
  const eresult = await db.query("SELECT id, type, opid, uid, project, aid, occurred_at, data FROM account_events WHERE aid = $1 AND occurred_at > $2 AND project = $3 AND deleted = false order by occurred_at;", [aid, since, project]);
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
  const vid         = event.vid;
  const maid        = event.maid;
  const opid        = event.opid;
  const type        = event.type;
  const amount      = event.amount;
  const occurred_at = event.occurred_at;
  const project     = event.project;

  // whether has event occurred?
  // if not, save and play it
  const eresult = oid ?
    await db.query("SELECT id FROM account_events WHERE aid = $1 AND uid = $2 AND type = $3 AND data::jsonb->>'oid' = $4 AND project = $5 AND deleted = false;", [aid, uid, type, oid, project]) :
    (maid ?
     await db.query("SELECT id FROM account_events WHERE aid = $1 AND uid = $2 AND type = $3 AND data::jsonb->>'maid' = $4 AND project = $5 AND deleted = false;", [aid, uid, type, maid, project])
     :
       { rowCount: 0 }
    );
  if (eresult.rowCount === 0) {
    // event not found
    if (type !== 0) {
      let data = null;
      if (vid) {
        if (oid) {
          data = { oid, amount, vid };
        } else {
          data = { maid, amount, vid };
        }
      } else {
        if (oid) {
          data = { oid, amount };
        } else {
          data = { maid, amount };
        }
      }
      if (event.license) {
        data = { ... data, license: event.license };
      }
      if (event.owner) {
        data = { ... data, owner: event.owner };
      }
      await db.query("INSERT INTO account_events (id, type, opid, uid, project, aid, occurred_at, data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);", [eid, type, opid, uid, project, aid, occurred_at, JSON.stringify(data)]);
      return await play_events(db, cache, aid, project);
    } else {
      const multi: Multi = bluebird.promisifyAll(cache.multi()) as Multi;
      if (event.project === 2 || event.project === 3) {
        multi.zrem(`accounts-${event.project}`, aid);
        const pkt: Buffer = await cache.hgetAsync("account-entities", aid);
        if (pkt) {
          const account: Account = await msgpack_decode_async(pkt) as Account;
          if (account.owner && account.owner.phone) {
            multi.zrem(`accounts-of-phone-${event.project}:${account.owner.phone}`, aid);
          }
          if (account.license) {
            multi.zrem(`accounts-of-license-${account.project}:${account.license}`, aid);
          }
        }
      }
      multi.hdel("account-entities", aid);
      await multi.execAsync(); // replay events
      return await play_events(db, cache, aid, project);
    }
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

  if (!event) {
    return { code: 400, msg: "AccountEvent is null" };
  }

  const type        = event.type;
  const oid         = event.oid;
  const aid         = event.aid;
  const maid        = event.maid;
  const opid        = event.opid;
  const vid         = event.vid;
  const amount      = event.amount;
  const occurred_at = event.occurred_at;
  const project     = event.project;
  let uid           = event.uid;

  log.info(`onEvent: id: ${event.id}, type: ${type}, oid: ${oid}, aid: ${aid}, maid: ${maid}, opid: ${opid}, vid: ${vid}, amount: ${amount}, occurred_at: ${occurred_at ? occurred_at.toISOString() : undefined}, uid: ${uid}, undo: ${event.undo}, project: ${project}`);
  const start: Date = new Date();

  if (!uid) {
    const result = await db.query("SELECT DISTINCT uid FROM account_events WHERE aid = $1;", [aid]);
    if (result.rowCount > 0) {
      uid = result.rows[0].uid;
      event.uid = uid;
    } else {
      // the account has been removed
      // find the correct uid and sync wallet
      const apkt = await cache.hgetAsync("account-entities", aid);
      if (apkt && type === 0) {
        const account = await msgpack_decode_async(apkt) as Account;
        const multi: Multi = bluebird.promisifyAll(cache.multi()) as Multi;
        multi.hdel("account-entities", aid);
        multi.zrem(`accounts-${account.project}`, aid);
        if (account.owner && account.owner.phone) {
          multi.zrem(`accounts-of-phone-${account.project}:${account.owner.phone}`, aid);
        }
        if (account.license) {
          multi.zrem(`accounts-of-license-${account.project}:${account.license}`, aid);
        }
        await multi.execAsync();
        const result = await sync_wallet(db, cache, account.uid, account.project);
        const stop: Date = new Date();
        log.info(`onEvent: id: ${event.id}, type: ${type}, oid: ${oid}, aid: ${aid}, maid: ${maid}, opid: ${opid}, vid: ${vid}, amount: ${amount}, occurred_at: ${occurred_at ? occurred_at.toISOString() : undefined}, uid: ${uid}, undo: ${event.undo}, project: ${project}, done in ${stop.getTime() - start.getTime()} milliseconds`);

        return result;
      } else {
        const stop: Date = new Date();
        log.info(`onEvent: id: ${event.id}, type: ${type}, oid: ${oid}, aid: ${aid}, maid: ${maid}, opid: ${opid}, vid: ${vid}, amount: ${amount}, occurred_at: ${occurred_at ? occurred_at.toISOString() : undefined}, uid: ${uid}, undo: ${event.undo}, project: ${project}, done in ${stop.getTime() - start.getTime()} milliseconds`);
        return { code: 404, msg: "用户不存在，无法执行事件: " + string_of_event_type(type) };
      }
    }
  }

  if (event.undo) {
    const result = handle_undo_event(db, cache, event);
    const stop: Date = new Date();
    log.info(`onEvent: id: ${event.id}, type: ${type}, oid: ${oid}, aid: ${aid}, maid: ${maid}, opid: ${opid}, vid: ${vid}, amount: ${amount}, occurred_at: ${occurred_at ? occurred_at.toISOString() : undefined}, uid: ${uid}, undo: ${event.undo}, project: ${project}, done in ${stop.getTime() - start.getTime()} milliseconds`);
    return result;
  } else {
    const result = handle_event(db, cache, event);
    const stop: Date = new Date();
    log.info(`onEvent: id: ${event.id}, type: ${type}, oid: ${oid}, aid: ${aid}, maid: ${maid}, opid: ${opid}, vid: ${vid}, amount: ${amount}, occurred_at: ${occurred_at ? occurred_at.toISOString() : undefined}, uid: ${uid}, undo: ${event.undo}, project: ${project}, done in ${stop.getTime() - start.getTime()} milliseconds`);
    return result;
  }
});
