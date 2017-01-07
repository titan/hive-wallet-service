import { Processor, ProcessorFunction, ProcessorContext, rpc, set_for_response, msgpack_encode, msgpack_decode } from "hive-service";
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

// { cmd: "createAccount", args: [domain, uid ? uid : ctx.uid, vid, pid, aid, cbflag] };
processor.call("createAccount", (ctx: ProcessorContext, domain: string, uid: string, vid: string, pid: string, aid: string, cbflag: string) => {
  log.info(`createAccount, domain: ${domain}, uid: ${uid}, vid: ${vid}, pid: ${pid}, aid: ${aid}, cbflag: ${cbflag}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  const wid = uid;
  (async () => {
    try {
      const accreps = await db.query("SELECT 1 FROM accounts WHERE vid = $1 AND pid = $2", [vid, pid]);
      if (accreps["rowCount"] === 0) {
        const result = await db.query("SELECT 1 FROM wallets WHERE uid = $1", [uid]);
        if (result["rowCount"] === 0) {
          const evtid: string = uuid.v4();
          const evt_type: number = 0;
          const data: Object = {
            id: evtid,
            type: evt_type,
            uid: uid,
            occurred_at: new Date()
          };
          await db.query("BEGIN");
          await db.query("INSERT INTO wallet_events(id, type, opid, uid, data) VALUES($1, $2, $3, $4, $5)", [evtid, evt_type, uid, uid, data]);
          await db.query("INSERT INTO wallets(id, uid, balance, evtid) VALUES($1, $2, $3, $4)", [wid, uid, 0, evtid]);
          await db.query("INSERT INTO accounts(id, uid, vid, pid, balance0, balance1, balance2) VALUES($1, $2, $3, $4, $5, $6, $7)", [aid, uid, vid, pid, 0, 0, 0]);
          await db.query("COMMIT");
        } else {
          await db.query("INSERT INTO accounts(id, uid, vid, pid, balance0, balance1, balance2) VALUES($1, $2, $3, $4, $5, $6, $7)", [aid, uid, vid, pid, 0, 0, 0]);
        }
      } else {
        await set_for_response(cache, cbflag, {
          code: 200,
          msg: "该对应帐号已存在"
        });
        done();
        return;
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
    await sync_wallets(db, cache, domain, wid);
    await set_for_response(cache, cbflag, {
      code: 200,
      data: { code: 200, data: aid }
    });
    done();
  })();
});

// 加上管理费支出与补贴交易记录
// args = [domain, vid, aid, pid, type0, type1, balance0, balance1, balance2, title, oid, cbflag, uid]
processor.call("updateAccountBalance", (ctx: ProcessorContext, domain: string, vid: string, aid: string, pid: string, type0: number, type1: number, balance0: number, balance1: number, balance2: number, title: string, order_id: string, uid: string, callback: string) => {
  log.info(`updateAccountBalance  domain:${domain}, vid:${vid}, pid:${pid}, type0:${type0}, type1:${type1}, balance0:${balance0}, balance1:${balance1}, balance2:${balance2}, title:${title}, order_id:${order_id}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  const created_at = new Date();
  const balance = balance0 + balance1;
  const title1 = "服务费扣除";
  const title2 = "服务费补贴";
  const type2 = -1; // 帐号扣除　(扣服务费)　
  const tid = uuid.v4();
  const evtid = uuid.v4();
  const data = {
    id: evtid,
    type: type1,
    uid: uid,
    occurred_at: created_at,
    amount: balance,
    oid: order_id,
    aid: aid
  };
  (async () => {
    try {
      await db.query("BEGIN");
      const wreps = await db.query("SELECT balance FROM wallets WHERE uid = $1", [uid]);
      if (wreps["rowCount"] !== 0) {
        const old_balance: number = parseFloat(wreps["rows"][0]["balance"]);
        await db.query("UPDATE wallets SET balance = $1, evtid = $2 WHERE id = $3", [balance + old_balance, evtid, uid]);
        const accreps = await db.query("SELECT balance0, balance1, balance2 FROM accounts WHERE id = $1 AND pid = $2", [aid, pid]);
        if (accreps["rowCount"] !== 0) {
          const old_balance0: number = parseFloat(accreps["rows"][0]["balance0"]);
          const old_balance1: number = parseFloat(accreps["rows"][0]["balance1"]);
          const old_balance2: number = parseFloat(accreps["rows"][0]["balance2"]);
          await db.query("UPDATE accounts SET balance0 = $1, balance1 = $2, balance2 = $3, updated_at = $4 WHERE id = $5 AND pid = $6", [balance0 + old_balance0, balance1 + old_balance1, balance2 + old_balance2, created_at, aid, pid]);
          await db.query("INSERT INTO transactions(id, aid, type, title, amount) VALUES($1, $2, $3, $4, $5)", [tid, aid, type0, title, balance]);
          await db.query("INSERT INTO transactions(id, aid, type, title, amount) VALUES($1, $2, $3, $4, $5)", [tid, aid, type2, title1, balance * 0.2]);
          await db.query("INSERT INTO transactions(id, aid, type, title, amount) VALUES($1, $2, $3, $4, $5)", [tid, aid, type0, title2, balance * 0.2]);
          await db.query("INSERT INTO wallet_events(id, type, opid, uid, data) VALUES($1, $2, $3, $4, $5)", [evtid, type1, uid, uid, data]);
          await db.query("COMMIT");
        } else {
          set_for_response(cache, callback, {
            code: 404,
            msg: "未找到对应帐号信息"
          });
        }
      } else {
        set_for_response(cache, callback, {
          code: 404,
          msg: "未找到对应钱包信息"
        });
      }
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
      const accounts = await msgpack_decode(wallet_entities);
      for (let account of accounts["accounts"]) {
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
      const transaction1 = { amount: balance, occurred_at: created_at, aid: aid, id: uid, title: title, type: type0 };
      const transaction2 = { amount: balance * 0.2, occurred_at: created_at, aid: aid, id: uid, title: title1, type: type2 };
      const transaction3 = { amount: balance * 0.2, occurred_at: created_at, aid: aid, id: uid, title2: title, type: type0 };
      const new_accounts = await msgpack_encode(accounts);
      const new_transaction1 = await msgpack_encode(transaction1);
      const new_transaction2 = await msgpack_encode(transaction2);
      const new_transaction3 = await msgpack_encode(transaction3);
      multi.hset("wallet-entities", uid, JSON.stringify(new_accounts));
      multi.zadd("transactions-" + uid, created_at.getTime(), JSON.stringify(transaction1));
      multi.zadd("transactions-" + uid, created_at.getTime(), JSON.stringify(transaction2));
      multi.zadd("transactions-" + uid, created_at.getTime(), JSON.stringify(transaction3));
      await multi.execAsync();
      await set_for_response(cache, callback, {
        code: 200,
        data: aid
      });
      done();
    } catch (e) {
      log.info(e);
      await set_for_response(cache, callback, {
        code: 500,
        msg: e.message
      });
      done();
      return;
    }
  })();
});


// { cmd: "freeze", args: [domain, ctx.uid, amount, maid, aid, cbflag] };

processor.call("freeze", (ctx: ProcessorContext, domain: string, uid: string, amount: number, maid: string, aid: string, type: number, cbflag: string) => {
  log.info(`freeze  domain : ${domain}, uid : ${uid}, amount : ${amount}, maid : ${maid}, aid : ${aid} `);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  let title = null;
  if (type === -4) {
    title = "小组个人余额冻结";
  } else if (type === -5) {
    title = "公共资金池余额冻结";
  }
  const updated_at = new Date();
  const type1: number = 2; // type表示交易类型 type1表示钱包事件类型
  const tid: string = uuid.v4();
  const evtid: string = uuid.v4();
  const data: Object = {
    id: evtid,
    type: type1,
    uid: uid,
    occurred_at: updated_at,
    amount: amount,
    aid: aid,
    maid: maid
  };
  (async () => {
    try {
      await db.query("BEGIN");
      const wreps = await db.query("SELECT frozen, balance updated_at FROM wallets WHERE id = $1", [uid]);
      if (wreps["rowCount"] !== 0) {
        const old_frozen: number = parseFloat(wreps["rows"][0]["frozen"]);
        const old_balance: number = parseFloat(wreps["rows"][0]["balance"]) - old_frozen;
        if (amount > old_balance) {
          set_for_response(cache, cbflag, {
            code: 500,
            msg: "冻结金额大于钱包余额"
          });
        } else {
          await db.query("INSERT INTO transactions(id, aid, type, title, amount) VALUES($1, $2, $3, $4,$5)", [tid, aid, type, title, amount]);
          await db.query("UPDATE wallets SET frozen = $1 WHERE uid = $2", [amount + old_frozen, uid]);
          await db.query("INSERT INTO wallet_events(id, type, opid, uid, occurred_at, data) VALUES($1, $2, $3, $4, $5, $6)", [evtid, type1, uid, uid, updated_at, data]);
          await db.query("COMMIT");
          await sync_wallets(db, cache, domain, uid);
          await sync_transitions(db, cache, domain, tid);
          set_for_response(cache, cbflag, {
            code: 200,
            data: "success"
          });
          done();
        }
      } else {
        set_for_response(cache, cbflag, {
          code: 404,
          msg: "未找到对应钱包信息"
        });
        done();
        return;
      }
    } catch (e) {
      log.info(e);
      set_for_response(cache, cbflag, {
        code: 500,
        msg: e.message
      });
      done();
    }
  })();
});

processor.call("unfreeze", (ctx: ProcessorContext, domain: string, uid: string, amount: number, maid: string, aid: string, type: number, cbflag: string) => {
  log.info(`unfreeze  domain : ${domain}, uid : ${uid}, amount : ${amount}, maid : ${maid}, aid : ${aid} `);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  let title = null;
  if (type === 4) {
    title = "小组个人余额解冻";
  } else if (type === 5) {
    title = "公共资金池余额解冻";
  }
  const updated_at = new Date();
  const type1: number = 3;　 // type表示交易类型 type1表示钱包事件类型
  const tid: string = uuid.v4();
  const evtid: string = uuid.v4();
  const data: Object = {
    id: evtid,
    type: type1,
    uid: uid,
    occurred_at: updated_at,
    amount: amount,
    aid: aid,
    maid: maid
  };
  (async () => {
    try {
      await db.query("BEGIN");
      const wreps = await db.query("SELECT frozen updated_at FROM wallets WHERE id = $1", [uid]);
      if (wreps["rowCount"] !== 0) {
        const old_frozen: number = parseFloat(wreps["rows"][0]["frozen"]);
        if (amount > old_frozen) {
          set_for_response(cache, cbflag, {
            code: 500,
            msg: "解冻金额大于钱包冻结金额"
          });
        } else {
          await db.query("INSERT INTO transactions(id, aid, type, title, amount) VALUES($1, $2, $3, $4,$5)", [tid, aid, type, title, amount]);
          await db.query("UPDATE wallets SET frozen = $1 WHERE uid = $2", [old_frozen - amount, uid]);
          await db.query("INSERT INTO wallet_events(id, type, opid, uid, occurred_at, data) VALUES($1, $2, $3, $4, $5, $6)", [evtid, type1, uid, uid, updated_at, data]);
          await db.query("COMMIT");
          await sync_wallets(db, cache, domain, uid);
          await sync_transitions(db, cache, domain, tid);
          set_for_response(cache, cbflag, {
            code: 200,
            data: "success"
          });
          done();
        }
      } else {
        set_for_response(cache, cbflag, {
          code: 404,
          msg: "未找到对应钱包信息"
        });
        done();
        return;
      }
    } catch (e) {
      log.info(e);
      set_for_response(cache, cbflag, {
        code: 500,
        msg: e.message
      });
      done();
    }
  })();
});


processor.call("debit", (ctx: ProcessorContext, domain: string, uid: string, amount: number, maid: string, cbflag: string) => {
  log.info(`debit  domain : ${domain}, uid : ${uid}, amount : ${amount}, maid : ${maid} `);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  const updated_at = new Date();
  const type: number = 4;
  const evtid: string = uuid.v4();
  const data: Object = {
    id: evtid,
    type: type,
    uid: uid,
    occurred_at: updated_at,
    amount: amount,
    maid: maid
  };
  (async () => {
    try {
      await db.query("BEGIN");
      const wreps = await db.query("SELECT balance updated_at FROM wallets WHERE id = $1", [uid]);
      if (wreps["rowCount"] !== 0) {
        const old_balance: number = parseFloat(wreps["rows"][0]["balance"]);
        if (old_balance > amount) {
          set_for_response(cache, cbflag, {
            code: 500,
            msg: "扣款金额大于钱包余额"
          });
        } else {
          await db.query("UPDATE wallets SET balance = $1 WHERE uid = $2", [old_balance - amount, uid]);
          await db.query("INSERT INTO wallet_events(id, type, opid, uid, occurred_at, data) VALUES($1, $2, $3, $4, $5, $6)", [evtid, type, uid, uid, updated_at, data]);
          await db.query("COMMIT");
          await sync_wallets(db, cache, domain, uid);
          set_for_response(cache, cbflag, {
            code: 200,
            data: "success"
          });
          done();
        }
      } else {
        set_for_response(cache, cbflag, {
          code: 404,
          msg: "未找到对应钱包信息"
        });
        done();
        return;
      }
    } catch (e) {
      log.info(e);
      set_for_response(cache, cbflag, {
        code: 500,
        msg: e.message
      });
      done();
    }
  })();
});

processor.call("cashin", (ctx: ProcessorContext, domain: string, uid: string, amount: number, oid: string, cbflag: string) => {
  log.info(`cashin  domain : ${domain}, uid : ${uid}, amount : ${amount}, oid : ${oid} `);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  const updated_at = new Date();
  const type: number = 5;
  const evtid: string = uuid.v4();
  const data: Object = {
    id: evtid,
    type: type,
    uid: uid,
    occurred_at: updated_at,
    amount: amount,
    oid: oid
  };
  (async () => {
    try {
      await db.query("BEGIN");
      const wreps = await db.query("SELECT cashable updated_at FROM wallets WHERE id = $1", [uid]);
      if (wreps["rowCount"] !== 0) {
        const old_cashable: number = parseFloat(wreps["rows"][0]["cashable"]);
        await db.query("UPDATE wallets SET cashable = $1 WHERE uid = $2", [old_cashable + amount, uid]);
        await db.query("INSERT INTO wallet_events(id, type, opid, uid, occurred_at, data) VALUES($1, $2, $3, $4,$5, $6)", [evtid, type, uid, uid, updated_at, data]);
        await db.query("COMMIT");
        await sync_wallets(db, cache, domain, uid);
        set_for_response(cache, cbflag, {
          code: 200,
          data: "success"
        });
        done();
      } else {
        set_for_response(cache, cbflag, {
          code: 404,
          msg: "未找到对应钱包信息"
        });
        done();
        return;
      }
    } catch (e) {
      log.info(e);
      set_for_response(cache, cbflag, {
        code: 500,
        msg: e.message
      });
      done();
    }
  })();
});

processor.call("cashout", (ctx: ProcessorContext, domain: string, uid: string, amount: number, cbflag: string) => {
  log.info(`cashout  domain : ${domain}, uid : ${uid}, amount : ${amount}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  const updated_at = new Date();
  const type: number = 6;
  const evtid: string = uuid.v4();
  const data: Object = {
    id: evtid,
    type: type,
    uid: uid,
    occurred_at: updated_at,
    amount: amount,
  };
  (async () => {
    try {
      await db.query("BEGIN");
      const wreps = await db.query("SELECT cashable updated_at FROM wallets WHERE id = $1", [uid]);
      if (wreps["rowCount"] !== 0) {
        const old_cashable: number = parseFloat(wreps["rows"][0]["cashable"]);
        if (old_cashable < amount) {
          set_for_response(cache, cbflag, {
            code: 500,
            msg: "申请提现金额大于可提现余额"
          });
        } else {
          await db.query("UPDATE wallets SET cashable = $1 WHERE uid = $2", [amount - old_cashable, uid]);
          await db.query("INSERT INTO wallet_events(id, type,opid, uid, occurred_at, data) VALUES($1, $2, $3, $4, $5, $6)", [evtid, type, uid, uid, updated_at, data]);
          await db.query("COMMIT");
          await sync_wallets(db, cache, domain, uid);
          set_for_response(cache, cbflag, {
            code: 200,
            data: "success"
          });
          done();
        }
      } else {
        set_for_response(cache, cbflag, {
          code: 404,
          msg: "未找到对应钱包信息"
        });
        done();
        return;
      }
    } catch (e) {
      log.info(e);
      set_for_response(cache, cbflag, {
        code: 500,
        msg: e.message
      });
      done();
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
  const result = await db.query("SELECT w.id AS wid, w.uid, w.frozen, w.cashable, w.balance, a.id AS aid, a.pid, a.vid, a.balance0, a.balance1, a.balance2 FROM wallets AS w INNER JOIN accounts AS a ON w.uid = a.uid WHERE w.deleted = false AND a.deleted = false " + (wid ? "AND w.uid = $1 ORDER BY wid, w.uid" : "ORDER BY wid, w.uid"), wid ? [wid] : []);
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
      id: row.aid,
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
    for (const account of wallet["accounts"]) {
      multi.hset("vid-aid", account["vid"] + account["pid"], account["id"]);
    }
    multi.hset("wallet-entities", wallet.id, pkt);
  }
  return multi.execAsync();
}

async function refresh_transitions(db, cache, domain: string): Promise<void> {
  return sync_transitions(db, cache, domain);
}

async function sync_transitions(db, cache, domain: string, tid?: string): Promise<void> {
  const multi = bluebird.promisifyAll(cache.multi()) as Multi;
  if (!tid) {
    const keys = await cache.keysAsync("transactions:*");
    for (const key of keys) {
      multi.del(key);
    }
    await multi.execAsync();
  }
  const result = await db.query("SELECT aid, type, title, amount, occurred_at FROM transactions", tid ? [tid] : []);
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
    try {
      const RW = await refresh_wallets(db, cache, domain);
      const RT = await refresh_transitions(db, cache, domain);
      log.info("refresh done!");
      done();
    } catch (e) {
      log.error(e);
      done();
    }
  })();
});

console.log("Start wallet processor");

