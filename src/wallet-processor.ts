"use strict";
import { Processor, ProcessorFunction, ProcessorContext, rpc } from "hive-service";
import { async_serial, async_serial_ignore } from "hive-processor";
import { Client as PGClient, QueryResult } from "pg";
import * as msgpack from "msgpack-lite";
import { servermap, triggermap } from "hive-hostmap";
import * as nanomsg from "nanomsg";
import * as http from "http";
import * as queryString from "querystring";
import { RedisClient } from "redis";
import { CustomerMessage } from "recommend-library";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import * as bluebird from "bluebird";


declare module "redis" {
    export interface RedisClient extends NodeJS.EventEmitter {
        incrAsync(key: string): Promise<any>;
        hgetAsync(key: string, field: string): Promise<any>;
        hsetAsync(key: string, field: string, value: string): Promise<any>;
        hincrbyAsync(key: string, field: string, value: number): Promise<any>;
        lpushAsync(key: string, value: string | number): Promise<any>;
        setexAsync(key: string, ttl: number, value: string): Promise<any>;
        zrevrangebyscoreAsync(key: string, start: number, stop: number): Promise<any>;
    }
    export interface Multi extends NodeJS.EventEmitter {
        execAsync(): Promise<any>;
    }
}

let log = bunyan.createLogger({
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
let wxhost = process.env["WX_ENV"] === "test" ? "dev.fengchaohuzhu.com" : "m.fengchaohuzhu.com";

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

processor.call("createAccount", (ctx: ProcessorContext, domain: any, uid: string, aid: string, type: string, vid: string, order_id: string, callback: string) => {
    log.info("createAccount");
    // TODO aid　不再使用vid
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const done = ctx.done;
    const evtid: string = uuid.v4();
    const evt_type: number = 0;
    const occurred_at = new Date();
    const created_at = new Date().getTime();
    const created_at1 = getLocalTime(created_at / 1000);
    const data: Object = {
        id: evtid,
        type: evt_type,
        uid: uid,
        occurred_at: occurred_at,
        oid: order_id,

        aid: aid
    };
    (async () => {
        try {
            await db.query("BEGIN");
            await db.query("INSERT INTO wallets(id,uid,balance,evtid) VALUES($1,$2,$3,$4)", [uid, uid, 0, vid, evtid]);
            await db.query("INSERT INTO accounts(id,uid,type,vid,balance0,balance1) VALUES($1,$2,$3,$4,$5,$6)", [aid, uid, type, vid, 0, 0]);
            await db.query("INSERT INTO wallet_events(id,type,uid,data) VALUES($1,$2,$3,$4)", [evtid, evt_type, uid, data]);
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
            const v = await rpc(domain, servermap["vehicle"], null, "getVehicle", vid);
            const wallet = await cache.hgetAsync("wallet-entities", uid);
            if (wallet === "" || wallet === null) {
                let accounts = [];
                let vehicle = v["data"];
                let account = { balance0: 0, balance1: 0, id: aid, type: type, vehicle: vehicle };
                accounts.push(account);
                const multi = cache.multi();
                multi.hset("wallet-entities", uid, JSON.stringify(accounts));
                multi.hset("vid-aid", vid, aid);
                await multi.execAsync();
                await cache.setexAsync(callback, 30, JSON.stringify({
                    code: 200,
                    data: { code: 200, aid: aid }
                }));
            } else {
                let accounts = JSON.parse(wallet);
                let vehicle = v["data"];
                let account = { balance0: 0, balance1: 0, id: aid, type: type, vehicle: vehicle };
                accounts.push(account);
                const multi = cache.multi();
                multi.hset("wallet-entities", uid, JSON.stringify(accounts));
                multi.hset("vid-aid", vid, aid);
                await multi.execAsync();
                await cache.setexAsync(callback, 30, JSON.stringify({
                    code: 200,
                    data: { code: 200, aid: aid }
                }));
            }
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
//  args: [domain, uid, vid, aid, type, type1, balance0, balance1, order_id, callback] };
processor.call("updateAccountbalance", (ctx: ProcessorContext, domain: any, uid: string, vid: string, aid: string, type: number, type1: number, balance0: number, balance1: number, order_id: string, title: string, callback: string) => {
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

function refresh_wallets(db, cache, done, domain: string): Promise<void> {
    return sync_wallets(db, cache, done, domain);
}
function sync_wallets(db, cache, done, domain): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        db.query("SELECT id, uid, type, vid, balance0, balance1, frozen, created_at, updated_at FROM accounts WHERE deleted = false", [], (err, result) => {
            if (err) {
                log.info(err);
                reject(err);
            } else {
                const allaccounts = [];
                const vids = [];
                // let account = { balance0: balance0, balance1: balance1, id: aid, type: type, vehicle: vehicle };
                for (let row of result.rows) {
                    let account = {
                        balance0: parseFloat(row.balance0),
                        balance1: parseFloat(row.balance1),
                        id: row.id,
                        uid: null,
                        type: row.type,
                        frozen: row.frozen,
                        vehicle: null
                    };
                    vids.push(row.id);
                    allaccounts.push(account);
                }
                let wvs = vids.map(vid => rpc<Object>("mobile", servermap["vehicle"], null, "getVehicle", vid));
                async_serial_ignore<Object>(wvs, [], (vreps) => {
                    const vehicles = vreps.filter(v => v["code"] === 200).map(v => v["data"]);
                    for (const vehicle of vehicles) {
                        for (const account of allaccounts) {
                            if (vehicle["id"] === account["id"]) {
                                account["vehicle"] = vehicle;
                                account["uid"] = vehicle["user_id"];
                            }
                        }
                    }
                    let wallets: Object = {};
                    const new_wallets: Object[] = [];
                    wallets = allaccounts.reduce((acc, account) => {
                        const uid = account["uid"];
                        if (acc[uid]) {
                            acc[uid].push(account);
                        } else {
                            acc[uid] = [account];
                        }
                        return acc;
                    }, {});
                    for (let wallet in wallets) {
                        new_wallets.push(wallets[wallet]);
                    }
                    let multi = cache.multi();
                    for (let new_wallet of new_wallets) {
                        multi.hset("wallet-entities", new_wallet[0]["uid"], JSON.stringify(new_wallet));
                    }
                    multi.exec((err, result) => {
                        if (err) {
                            reject(err);
                        } else {
                            resolve();
                        }
                    });
                });
            }
        });
    });
}
function refresh_transitions(db, cache, done, domain: string) {
    return sync_transitions(db, cache, done, domain);
}
function sync_transitions(db, cache, done, domain: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        db.query("SELECT aid, type, title, amount, occurred_at FROM transactions", [], (err, result) => {
            if (err) {
                log.info(err);
                reject(err);
            } else {
                const transactions = [];
                const vids = [];
                for (let row of result.rows) {
                    let transaction = {
                        id: null,
                        amount: parseFloat(row.amount),
                        occurred_at: row.occurred_at.toLocaleString(),
                        aid: row.aid,
                        title: trim(row.title),
                        type: row.type
                    };
                    vids.push(row.aid);
                    transactions.push(transaction);
                }
                let tvs = vids.map(vid => rpc<Object>("mobile", servermap["vehicle"], null, "getVehicle", vid));
                async_serial_ignore<Object>(tvs, [], (vreps) => {
                    const vehicles = vreps.filter(v => v["code"] === 200).map(v => v["data"]);
                    for (const vehicle of vehicles) {
                        for (const transaction of transactions) {
                            if (vehicle["id"] === transaction["aid"]) {
                                transaction["id"] = vehicle["user_id"];
                            }
                        }
                    }
                    let multi = cache.multi();
                    for (let transaction of transactions) {
                        multi.zadd("transactions-" + transaction["id"], new Date(transaction["occurred_at"]), JSON.stringify(transaction));
                    }
                    multi.exec((err, result) => {
                        if (err) {
                            log.info(err);
                            reject(err);
                        } else {
                            resolve();
                        }
                    });
                });
            }
        });
    });
}

processor.call("refresh", (ctx: ProcessorContext, domain: string) => {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const done = ctx.done;
    const RW = refresh_wallets(db, cache, done, domain);
    const RT = refresh_transitions(db, cache, done, domain);
    let ps = [RW, RT];
    async_serial_ignore<void>(ps, [], () => {
        log.info("refresh done!");
        done();
    });
});
processor.call("applyCashOut", (db: PGClient, cache1: RedisClient, done: DoneFunction, domain: any, order_id: string, user_id: string, cbflag: string) => {
    log.info("applyCashOut");
    const cache = bluebird.promisifyAll(cache1) as RedisClient;
    let date = new Date();
    let year = date.getFullYear();
    let month = (date.getMonth() + 1) < 10 ? "0" + (date.getMonth() + 1) : (date.getMonth() + 1);
    let day = date.getDate() < 10 ? "0" + date.getDate() : date.getDate();
    let dateString: string = year + "" + month + "" + day;
    let cash_no: string = dateString;
    let state = 0;
    let coid = uuid.v1();
    (async () => {
        try {
            const cash_counter: string = await cache.hincrbyAsync("cashout-counter", dateString, 1);
            for (let i = 0, len = 4 - cash_counter.toString().length; i < len; i++) {
                cash_no += "0";
            }
            cash_no += cash_counter;

            const accountsjson: string = await cache.hgetAsync("wallet-entities", user_id);
            const vid: string = await cache.hgetAsync("orderid-vid", order_id);
            const accounts: Object[] = JSON.parse(accountsjson);
            const amount = accounts.filter(x => x["vehicle"]["id"] === vid).reduce((acc, x) => x["balance0"] + x["balance1"], 0); // only one item

            const cashout_entity = {
                id: coid,
                no: cash_no,
                state: state,
                amount: amount,
                reason: null,
                order_id: order_id,
                last_event_id: null,
                created_at: date,
                updated_at: date
            };
            log.info(cashout_entity);
            let multi = bluebird.promisifyAll(cache.multi()) as Multi;
            multi.hset("cashout-entities", coid, JSON.stringify(cashout_entity));
            multi.zadd("applied-cashouts", date.getTime(), coid);
            await multi.execAsync();
            const myuuid = await UUID.v3({ namespace: UUID.namespace.url, name: cashout_entity["no"] + cashout_entity["updated_at"] + cashout_entity["state"].toString() });
            await db.query("INSERT INTO cashout(id, no, state, amount, order_id, last_event_id) VALUES($1, $2, $3, $4, $5, $6)", [coid, cash_no, state, amount, order_id, myuuid]);
            await db.query("INSERT INTO cashout_events(id, type, opid, uid, data) VALUES ($1, $2, $3, $4, $5)", [myuuid, cashout_entity["state"], user_id, user_id, cashout_entity])
            await cache.setexAsync(cbflag, 30, JSON.stringify({ code: 200, data: coid }));
            done();
        } catch (e) {
            log.error(e);
            await cache.setexAsync(cbflag, 30, JSON.stringify({ code: 500, msg: e.message }));
            done()
        }
    })();
});

processor.call("agreeCashOut", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: any, coid: string, state: number, opid: string, user_id: string, cbflag: string) => {
    log.info("agreeCashOut");
    (async () => {
        try {
            const date = new Date();
            const cashoutjson: string = await cache.hgetAsync("cashout-entities", coid);
            let cashout: Object = JSON.parse(cashoutjson);
            cashout["state"] = state;
            cashout["updated_at"] = date;
            const cashout_entity = cashout;
            const myuuid = await UUID.v3({ namespace: UUID.namespace.url, name: cashout_entity["no"] + cashout_entity["updated_at"] + cashout_entity["state"].toString() });
            await db.query("UPDATE cashout SET state = $1 , updated_at = $2, last_event_id = $3 WHERE id = $4 AND deleted = false", [state, date, myuuid, coid]);
            let multi = bluebird.promisifyAll(cache.multi()) as Multi;
            multi.hset("cashout-entities", coid, JSON.stringify(cashout));
            multi.zrem("applied-cashouts", coid);
            if (state === 1) {
                multi.zadd("agreed-cashouts", date.getTime(), coid);
            }
            if (state === 2) {
                multi.zadd("refused-cashouts", date.getTime(), coid);
            }
            await multi.execAsync();
            await db.query("INSERT INTO cashout_events(id, type, opid, uid, data) VALUES ($1, $2, $3, $4, $5)", [myuuid, cashout_entity["state"], opid, user_id, cashout_entity]);
            const orderjson: string = await cache.hgetAsync("order-entities", cashout_entity["order_id"]);
            const order_no: Object = JSON.parse(orderjson)["id"];

            if (state === 1) {
                const test = process.env["WX_ENV"] === "test" ? true : false;
                const profile = await rpc(domain, servermap["profile"], null, "getUserByUserId", user_id);
                const payment = await rpc(domain, servermap["bank_payment"], null, "getCustomerId", profile["data"]["pnrid"]);
                const bank_amount = cashout_entity["amount"].toFixed(2).toString();
                const generate = await rpc(domain, servermap["bank_payment"], null, "generateCashUrl", order_no, payment["cid"], bank_amount, test);
                const url = generate["url"];
                const order = await rpc(domain, servermap["order"], null, "updateOrderState", user_id, order_no, 6, "待退款");
                const postData = queryString.stringify({ "user": profile["data"]["openid"], "amount": cashout_entity["amount"], "url": url });
                const options = {
                    hostname: wxhost, port: 80, path: "/wx/wxpay/tmsgCashOut", method: "GET",
                    headers: {
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Content-Length": Buffer.byteLength(postData)
                    }
                };
                const req = await http.request(options, (res) => {
                    log.info(`STATUS: ${res.statusCode}`);
                    res.setEncoding("utf8");
                    res.on("data", (chunk) => {
                        log.info(`BODY: ${chunk}`);
                    });
                    res.on("end", () => {
                        log.info("agreeCashOut success");
                    });
                });
                req.write(postData);
                req.end();
                await cache.setexAsync(cbflag, 30, JSON.stringify({ code: 200, data: coid }));
                done();
            } else {
                await cache.setexAsync(cbflag, 30, JSON.stringify({ code: 200, data: coid }));
                done();
            }
        } catch (e) {
            log.error(e);
            await cache.setexAsync(cbflag, 30, JSON.stringify({ code: 500, msg: e.message }));
            done();
        }
    })();
});
processor.run();
console.log("Start processor at " + config.addr);







