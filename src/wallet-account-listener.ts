import { BusinessEventContext, BusinessEventHandlerFunction, BusinessEventListener, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, waitingAsync, msgpack_decode_async, msgpack_encode_async } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import * as bluebird from "bluebird";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import * as Disq from "hive-disque";
import { AccountEvent, Account, Wallet } from "./wallet-define";

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

function row2account(row): Account {
  return {
    id:               row.id,
    vid:              row.vid,
    uid:              row.uid,
    balance0:         parseFloat(row.balance0),
    balance1:         parseFloat(row.balance1),
    bonus:            parseFloat(row.bonus),
    frozen_balance0:  parseFloat(row.frozen_balance0),
    frozen_balance1:  parseFloat(row.frozen_balance1),
    cashable_balance: parseFloat(row.cashable_balance),
    evtid:            row.evtid,
    created_at:       row.created_at,
    updated_at:       row.updated_at,
  };
}

function row2event(row): AccountEvent {
  let event = {
    id:          row.id,
    type:        row.type,
    opid:        row.opid,
    uid:         row.uid,
    aid:         row.aid,
    occurred_at: row.occurred_at,
    amount:      0.0
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

async function sync_account(db: PGClient, cache: RedisClient, account: any) {
  if (account) {
    const aeresult = await db.query("SELECT 1 FROM accounts WHERE aid = $1;", [account.id]);
    if (aeresult.rowCount === 0) {
      await db.query("INSERT INTO accounts (id, vid, uid, balance0, balance1, bonus, frozen_balance0, frozen_balance1, cashable_balance, evtid, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);", [account.id, account.vid, account.uid, account.balance0, account.balance1, account.bonus, account.frozen_balance0, account.frozen_balance1, account.cashable_balance, account.evtid, account.created_at, account.updated_at]);
    } else {
      await db.query("UPDATE accounts SET balance0 = $1, balance1 = $2, bonus = $3, frozen_balance0 = $4, frozen_balance1 = $5, cashable_balance = $6, evtid = $7, created_at = $8, updated_at = $9 WHERE aid = $10;", [account.balance0, account.balance1, account.bonus, account.frozen_balance0, account.frozen_balance1, account.cashable_balance, account.evtid, account.created_at, account.updated_at, account.id]);
    }
    let wallet = null;
    const wbuf = await cache.hgetAsync("wallet-entities", account.uid);
    if (wbuf) {
      wallet = await msgpack_decode_async(wbuf);
      if (wallet.accounts) {
        const accounts = [ account ];
        for (const a of wallet.accounts) {
          if (a.id !== account.id) {
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
      }
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
    wallet.cashable = wallet.cashable;
    wallet.balance  = wallet.balance;

    const wpkt = await msgpack_encode_async(wallet);
    await cache.hsetAsync("wallet-entities", account.uid, wpkt);
    return { code: 200, data: "Okay" };
  } else {
    return { code: 500, msg: "无法从事件流中合成帐号" };
  }
}

function play(account: Account, event: AccountEvent) {
  if (!account && event.type !== 0) {
    return null;
  }
  switch (event.type) {
    case  0: {
      if (event.vid) {
        return {
          id:               event.aid,
          vid:              event.vid,
          uid:              event.uid,
          balance0:         0.0,
          balance1:         0.0,
          bonus:            0.0,
          frozen_balance0:  0.0,
          frozen_balance1:  0.0,
          cashable_balance: 0.0,
          evtid:            event.id,
          created_at:       event.occurred_at,
          updated_at:       event.occurred_at,
        };
      } else {
        return null;
      }
    }
    case  1: return { ... account, evtid: event.id, updated_at: event.occurred_at, cashable_balance: account.cashable_balance + event.amount };
    case  2: return { ... account, evtid: event.id, updated_at: event.occurred_at, cashable_balance: account.cashable_balance - event.amount };
    case  3: return { ... account, evtid: event.id, updated_at: event.occurred_at, balance0: account.balance0 + event.amount };
    case  4: return { ... account, evtid: event.id, updated_at: event.occurred_at, balance0: account.balance0 - event.amount };
    case  5: return { ... account, evtid: event.id, updated_at: event.occurred_at, balance1: account.balance1 + event.amount };
    case  6: return { ... account, evtid: event.id, updated_at: event.occurred_at, balance1: account.balance1 - event.amount };
    case  7: return { ... account, evtid: event.id, updated_at: event.occurred_at, bonus: account.bonus + event.amount };
    case  8: return { ... account, evtid: event.id, updated_at: event.occurred_at, bonus: account.bonus - event.amount };
    case  9: return { ... account, evtid: event.id, updated_at: event.occurred_at, balance0: account.balance0 - event.amount, frozen_balance0: account.frozen_balance0 + event.amount };
    case 10: return { ... account, evtid: event.id, updated_at: event.occurred_at, balance0: account.balance0 + event.amount, frozen_balance0: account.frozen_balance0 - event.amount };
    case 11: return { ... account, evtid: event.id, updated_at: event.occurred_at, balance1: account.balance1 - event.amount, frozen_balance1: account.frozen_balance1 + event.amount };
    case 12: return { ... account, evtid: event.id, updated_at: event.occurred_at, balance1: account.balance1 + event.amount, frozen_balance1: account.frozen_balance1 - event.amount };
  }
}

async function play_events(db: PGClient, cache: RedisClient, aid: string) {
  // 1. detect how many events haven't been played
  let since = null;
  const result0 = await db.query("SELECT e.occurred_at FROM accounts AS a INNER JOIN account_events AS e ON a.evtid = e.id WHERE a.id = $1;", [aid]);
  if (result0.rowCount !== 0) {
    since = result0.rows[0].occurred_at;
  } else {
    since = new Date(0);
  }
  const eresult = await db.query("SELECT id FROM account_events WHERE aid = $1 AND occurred_at > $2 AND deleted = false;", [aid, since]);
  if (eresult.rowCount > 0) {
    // 2. get the snapshot of the account
    const aresult = await db.query("SELECT id, vid, uid, balance0, balance1, bonus, frozen_balance0, frozen_balance1, cashable_balance, evtid, created_at, updated_at FROM accounts WHERE aid = $1;", [aid]);
    if (aresult.rowCount > 0) {
      // got the account
      let account = row2account(aresult.rows[0]);
      for (const row of eresult.rows) {
        const event = row2event(row);
        account = play(account, event);
      }
      // 3. sync the snapshot of the account
      return await sync_account(db, cache, account);
    } else {
      // account not existed
      let account = null;
      for (const row of eresult.rows) {
        const event = row2event(row);
        account = play(account, event);
      }
      // 3. sync the snapshot of the account
      return await sync_account(db, cache, account);
    }
  } else {
    // no event need to play
    return { code: 200, data: "没有可以执行的事件" };
  }
}

listener.onEvent(async function (ctx: BusinessEventContext, data: any) {
  const type = data["type"];

  switch (type) {
    case 3:
    case 5:
      return await recharge(ctx, data);
    default:
      break;
  }
});

async function recharge(ctx: BusinessEventContext, event: AccountEvent): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;

  const type        = event.type;
  const oid         = event.oid;
  const uid         = event.uid;
  const opid        = event.opid;
  const vid         = event.vid;
  const amount      = event.amount;
  const occurred_at = event.occurred_at;
  let aid           = null;


	// whether does account exist?
  // if not, create it
  const aresult = await db.query("SELECT id FROM accounts WHERE uid = $1 AND vid = $2;", [uid, vid]);
  if (aresult.rowCount === 0) {
    const aresult1 = await db.query("SELECT id, aid FROM account_events WHERE uid = $1 AND vid = $2 AND type = 0;", [uid, vid]);
    if (aresult1.rowCount === 0) {
			// no account at all, create it
			aid = uuid.v4();
			await db.query("INSERT INTO account_events (id, type, opid, uid, aid, occurred_at, data) VALUES ($1, $2, $3, $4, $5, $6, $7);", [uuid.v4(), 0, opid, uid, aid, new Date(occurred_at.getTime() - 10), JSON.stringify({ vid: vid })]);
		} else {
			aid = aresult1.rows[0].aid;
		}
  } else {
    aid = aresult.rows[0].id;
  }

  // whether has event occurred?
  // if not, save and play it
  const eresult = await db.query("SELECT id FROM account_events WHERE aid = $1 AND uid = $2 AND type = $3 AND data::jsonb->>'oid' = $4 AND deleted = false;", [aid, uid, type, oid]);
  if (eresult.rowCount === 0) {
    // event not found
    await db.query("INSERT INTO account_events (id, type, opid, uid, aid, occurred_at, data) VALUES ($1, $2, $3, $4, $5, $6, $7);", [uuid.v4(), type, opid, uid, aid, occurred_at, JSON.stringify({ oid , amount })]);
    return await play_events(db, cache, aid);
  } else {
    return { code: 208, msg: "重复充值请求" }
  }
}
