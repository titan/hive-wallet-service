"use strict";
import { Processor, Config, ModuleFunction, DoneFunction, rpc, async_serial, async_serial_ignore } from "hive-processor";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import { servermap, triggermap } from "hive-hostmap";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import * as msgpack from "msgpack-lite";
import * as nanomsg from "nanomsg";
import {CustomerMessage} from "recommend-library";
import * as UUID from "uuid-1345";
import * as queryString from "querystring";
import * as http from "http";
import * as bluebird from "bluebird";

declare module "redis" {
    export interface RedisClient extends NodeJS.EventEmitter {
        hgetAsync(key: string, field: string): Promise<any>;
        hincrbyAsync(key: string, field: string, value: number): Promise<any>;
        setexAsync(key: string, ttl: number, value: string): Promise<any>;
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

let config: Config = {
    dbhost: process.env["DB_HOST"],
    dbuser: process.env["DB_USER"],
    dbport: process.env["DB_PORT"],
    database: process.env["DB_NAME"],
    dbpasswd: process.env["DB_PASSWORD"],
    cachehost: process.env["CACHE_HOST"],
    addr: "ipc:///tmp/wallet.ipc"
};
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


let processor = new Processor(config);
processor.call("createAccount", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: any, uid: string, aid: string, type: string, vid: string, balance0: number, balance1: number) => {
    log.info("createAccount");
    let balance = balance0 + balance1;
    let tid = uuid.v1();
    let title = `参加计划 收入`;
    let created_at = new Date().getTime();
    let created_at1 = getLocalTime(created_at / 1000);
    db.query("BEGIN", (err: Error) => {
        if (err) {
            log.error(err, "query error");
            done();
        } else {
            db.query("INSERT INTO accounts(id,uid,type,vid,balance0,balance1) VALUES($1,$2,$3,$4,$5,$6)", [aid, uid, type, vid, balance0, balance1], (err: Error) => {
                if (err) {
                    db.query("ROLLBACK", [], (err) => {
                        log.error(err, "insert into accounts error");
                        done();
                    });
                }
                else {
                    db.query("INSERT INTO transactions(id,aid,type,title,amount) VALUES($1,$2,$3,$4,$5)", [tid, aid, type, title, balance], (err: Error) => {
                        if (err) {
                            db.query("ROLLBACK", [], (err) => {
                                log.error(err, "insert into transactions error");
                                done();
                            });
                        }
                        else {
                            db.query("COMMIT", [], (err: Error) => {
                                if (err) {
                                    log.info(err);
                                    log.error(err, "insert plan order commit error");
                                    done();
                                } else {
                                    let p = rpc(domain, servermap["vehicle"], null, "getVehicle", vid);
                                    p.then((v) => {
                                        if (err) {
                                            done();
                                            log.info("call vehicle error");
                                        } else {
                                            cache.hget("wallet-entities", uid, function (err, result2) {
                                                if (err) {
                                                    log.info("get wallete-entities err");
                                                    done();
                                                } else if (result2 === "" || result2 === null) {
                                                    let accounts = [];
                                                    let vehicle = v["data"];
                                                    let multi = cache.multi();
                                                    let transactions = { amount: balance, occurred_at: created_at1, aid: aid, id: uid, title: title, type: 1 };
                                                    let account = { balance0: balance0, balance1: balance1, id: aid, type: type, vehicle: vehicle };
                                                    accounts.push(account);
                                                    multi.zadd("transactions-" + uid, created_at, JSON.stringify(transactions));
                                                    multi.hset("wallet-entities", uid, JSON.stringify(accounts));
                                                    multi.exec((err3, replies) => {
                                                        if (err3) {
                                                            log.error(err3, "query redis error");
                                                            done();
                                                        } else {
                                                            log.info("placeAnDriverOrder:==========is done");
                                                            done(); // close db and cache connection
                                                        }
                                                    });
                                                } else {
                                                    let accounts = JSON.parse(result2);
                                                    let vehicle = v["data"];
                                                    let multi = cache.multi();
                                                    let transactions = { amount: balance, occurred_at: created_at1, aid: aid, id: uid, title: title, type: 1 };
                                                    let account = { balance0: balance0, balance1: balance1, id: aid, type: type, vehicle: vehicle };
                                                    accounts.push(account);
                                                    multi.zadd("transactions-" + uid, created_at, JSON.stringify(transactions));
                                                    multi.hset("wallet-entities", uid, JSON.stringify(accounts));
                                                    multi.exec((err3, replies) => {
                                                        if (err3) {
                                                            log.error(err3, "query redis error");
                                                            done();
                                                        } else {
                                                            log.info("==========is done");
                                                            done(); // close db and cache connection
                                                        }
                                                    });
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    });
                }
            });
        }
    });
});

processor.call("updateAccountbalance", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: any, uid: string, vid: string, type1: string, balance0: number, balance1: number) => {
    log.info("updateOrderState");
    let created_at = new Date().getTime();
    let created_at1 = getLocalTime(created_at / 1000);
    let balance = balance0 + balance1;
    let title = `添加司机`;
    let tid = uuid.v1();
    db.query("BEGIN", (err: Error) => {
        if (err) {
            log.error(err, "query error");
            done();
        } else {
            db.query("UPDATE accounts SET balance0 = balance0 + $1,balance1 = balance1 + $2 WHERE id = $3", [balance0, balance1, vid], (err: Error, result: QueryResult) => {
                if (err) {
                    db.query("ROLLBACK", [], (err) => {
                        log.error(err, "insert into accounts error");
                        done();
                    });
                }
                else {
                    db.query("INSERT INTO transactions(id,aid,type,title,amount) VALUES($1,$2,$3,$4,$5)", [tid, vid, type1, title, balance], (err: Error, result: QueryResult) => {
                        if (err) {
                            db.query("ROLLBACK", [], (err) => {
                                log.error(err, "insert into accounts error");
                                done();
                            });
                        }
                        else {
                            db.query("COMMIT", [], (err: Error) => {
                                if (err) {
                                    log.info(err);
                                    log.error(err, "insert plan order commit error");
                                    done();
                                } else {
                                    let multi = cache.multi();
                                    multi.hget("wallet-entities", uid);
                                    multi.exec((err, replise: string) => {
                                        if (err) {
                                            log.info("err,get redis error");
                                            done();
                                        } else {
                                            log.info("================" + replise);
                                            let accounts = JSON.parse(replise);
                                            log.info(accounts);
                                            for (let account of accounts) {
                                                if (account["id"] === vid) {
                                                    let balance01 = account["balance0"];
                                                    let balance11 = account["balance1"];
                                                    let balance02 = balance01 + balance0;
                                                    let balance12 = balance11 + balance1;
                                                    account["balance0"] = balance02;
                                                    account["balance1"] = balance12;
                                                }
                                            }
                                            let multi = cache.multi();
                                            let transactions = { amount: balance, occurred_at: created_at1, aid: vid, id: uid, title: title, type: type1 };
                                            multi.hset("wallet-entities", uid, JSON.stringify(accounts));
                                            multi.zadd("transactions-" + uid, created_at, JSON.stringify(transactions));
                                            multi.exec((err, result1) => {
                                                if (err) {
                                                    log.info("err:hset order_entities error");
                                                    done();
                                                } else {
                                                    log.info("db end in updateOrderState");
                                                    done();
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    });
                }
            });
        }
    });
});

function refresh_wallets(db: PGClient, cache: RedisClient, done: DoneFunction, domain: string): Promise<void> {
    return sync_wallets(db, cache, done, domain);
}
function sync_wallets(db: PGClient, cache: RedisClient, done: DoneFunction, domain: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        db.query("SELECT id, uid, type, vid, balance0, balance1, created_at, updated_at FROM accounts WHERE deleted = false", [], (err, result) => {
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
function refresh_transitions(db: PGClient, cache: RedisClient, done: DoneFunction, domain: string) {
    return sync_transitions(db, cache, done, domain);
}
function sync_transitions(db: PGClient, cache: RedisClient, done: DoneFunction, domain: string): Promise<void> {
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

processor.call("refresh", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: string) => {
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

            await db.query("INSERT INTO cashout(id, no, state, amount, order_id) VALUES($1, $2, $3, $4, $5)", [coid, cash_no, state, amount, order_id]);
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
            await cashout_events_async(db, cache, cashout_entity, user_id, user_id);
            await cache.setexAsync(cbflag, 30, JSON.stringify({ code: 200, data: coid }));
        } catch (e) {
            log.error(e);
            await cache.setexAsync(cbflag, 30, JSON.stringify({ code: 500, msg: e.message }));
        }
    })();
});

async function cashout_events_async(db: PGClient, cache: RedisClient, cashout_entity: Object, opid: string, user_id: string) {
    const p = new Promise<any>((resolve, reject) => {
        UUID.v3({ namespace: UUID.namespace.url, name: cashout_entity["no"] + cashout_entity["updated_at"] + cashout_entity["state"].toString() }, (err, result) => {
            if (err) {
                reject(err);
            } else {
                resolve(result);
            }
        });
    });
    return p.then((myuuid: String) => {
        return db.query("INSERT INTO cashout_events(id, type, opid, uid, data) VALUES ($1, $2, $3, $4, $5)", [myuuid, cashout_entity["state"], opid, user_id, cashout_entity]);
    });
}

function cashout_events(db: PGClient, cache: RedisClient, done: DoneFunction, domain: any, cashout_entity: Object, opid: string, user_id: string, cb) {
    UUID.v3({ namespace: UUID.namespace.url, name: cashout_entity["no"] + cashout_entity["updated_at"] + cashout_entity["state"].toString() }, (err, result) => {
        if (err) {
            log.info("uuid v3 error " + err);
        } else {
            let result = uuid.v1();
            db.query("INSERT INTO cashout_events(id, type, opid, uid, data) VALUES ($1, $2, $3, $4, $5)", [result, cashout_entity["state"], opid, user_id, cashout_entity], (err2, result2) => {
                if (err2) {
                    log.info(err2, "Insert into cashout_events error");
                    cb(false);
                } else {
                    cb(true);
                }
            });
        }
    });
}

processor.call("agreeCashOut", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: any, coid: string, state: number, opid: string, user_id: string, cbflag: string) => {
    log.info("agreeCashOut");
    let date = new Date();
    let cashout_entity = {};
    let order_no = "";
    let url = "";
    let pcash = new Promise<void>((resolve, reject) => {
        db.query("UPDATE cashout SET state = $1 , updated_at = $2 where id = $3 AND deleted = false", [state, date, coid], (err, result) => {
            if (err) {
                log.error(err, "Error on UPDATE 'cashout'");
                reject(err);
            } else {
                resolve();
            }
        });
    });
    pcash.then(() => {
        let predis = new Promise<void>((resolve, reject) => {
            cache.hget("cashout-entities", coid, (err, result) => {
                if (err) {
                    reject(err);
                    log.info(err);
                } else if (result) {
                    let cashout = JSON.parse(result);
                    cashout["state"] = state;
                    cashout["updated_at"] = date;
                    cashout_entity = cashout;
                    let multi = cache.multi();
                    multi.hset("cashout-entities", coid, JSON.stringify(cashout));
                    multi.zrem("applied-cashouts", coid);
                    if (state === 1) {
                        multi.zadd("agreed-cashouts", date.getTime(), coid);
                    }
                    if (state === 2) {
                        multi.zadd("refused-cashouts", date.getTime(), coid);
                    }
                    multi.exec((err2, result2) => {
                        if (err2) {
                            reject(err2);
                            log.info(err2);
                        } else {
                            resolve();
                        }
                    });
                } else {
                    reject("Hget cashout-entities error");
                    log.info("Hget cashout-entities error");
                }
            });
        });
        predis.then(() => {
            let pevent = new Promise<void>((resolve, reject) => {
                cashout_events(db, cache, done, domain, cashout_entity, user_id, user_id, (cb) => {
                    if (cb) {
                        cache.hget("order-entities", cashout_entity["order_id"], (err, result) => {
                            if (err) {
                                reject(err);
                                log.info(err);
                            } else if (result) {
                                order_no = JSON.parse(result)["id"];
                                resolve();
                            } else {
                                reject("Hget order-entities error");
                                log.info("Hget order-entities error");
                            }
                        });
                    } else {
                        reject("Insert event error");
                        log.info("Insert event error");
                    }
                });
            });
            pevent.then(() => {
                let pbank = new Promise<void>((resolve, reject) => {
                    if (state === 1) {
                        let test = process.env["WX_ENV"] === "test" ? true : false;
                        let p = rpc(domain, servermap["profile"], null, "getUserByUserId", user_id);
                        p.then(profile => {
                            if (profile["code"] === 200 && profile["data"]["pnrid"]) {
                                let b = rpc(domain, servermap["bank_payment"], null, "getCustomerId", profile["data"]["pnrid"]);
                                b.then(payment => {
                                    if (payment["code"] === 200) {
                                        let bank_amount = cashout_entity["amount"].toFixed(2).toString();
                                        log.info(cashout_entity["order_id"], payment["cid"], bank_amount, test);
                                        let g = rpc(domain, servermap["bank_payment"], null, "generateCashUrl", order_no, payment["cid"], bank_amount, test);
                                        g.then(generate => {
                                            if (generate["code"] === 200) {
                                                url = generate["url"];
                                                log.info(url);
                                                resolve();
                                            } else {
                                                reject("Rpc generateCashUrl: " + generate["code"]);
                                            }
                                        }).catch((e: Error) => {
                                            reject(e);
                                        });
                                    } else {
                                        reject("Rpc getCustomerId: " + payment["code"]);
                                    }
                                }).catch((e: Error) => {
                                    reject(e);
                                });
                            } else {
                                reject("Rpc getUserByUserId: " + profile["code"]);
                            }
                        }).catch((e: Error) => {
                            reject(e);
                        });
                    } else {
                        resolve();
                    }
                });
                pbank.then(() => {
                    if (state === 1) {
                        let o = rpc(domain, servermap["order"], null, "updateOrderState", user_id, order_no, 6, "待退款");
                        o.then((order) => {
                            if (order["code"] === 200) {
                                let p = rpc(domain, servermap["profile"], null, "getUserByUserId", user_id);
                                p.then((profile) => {
                                    if (profile["code"] === 200) {
                                        let postData = queryString.stringify({
                                            "user": profile["data"]["openid"],
                                            "amount": cashout_entity["amount"],
                                            "url": url
                                        });
                                        let options = {
                                            hostname: wxhost,
                                            port: 80,
                                            path: "/wx/wxpay/tmsgCashOut",
                                            method: "GET",
                                            headers: {
                                                "Content-Type": "application/x-www-form-urlencoded",
                                                "Content-Length": Buffer.byteLength(postData)
                                            }
                                        };
                                        let req = http.request(options, (res) => {
                                            log.info(`STATUS: ${res.statusCode}`);
                                            res.setEncoding("utf8");
                                            res.on("data", (chunk) => {
                                                log.info(`BODY: ${chunk}`);
                                            });
                                            res.on("end", () => {
                                                log.info("agreeCashOut success");
                                                cache.setex(cbflag, 30, JSON.stringify({
                                                    code: 200,
                                                    coid: coid
                                                }), (err, result) => {
                                                    done();
                                                });
                                            });
                                        });
                                        req.on("error", (e) => {
                                            log.info(`problem with request: ${e.message}`);
                                        });
                                        req.write(postData);
                                        req.end();
                                    } else {
                                        errorDone(cache, done, cbflag, JSON.stringify(profile));
                                    }
                                });
                            } else {
                                errorDone(cache, done, cbflag, JSON.stringify(order));
                            }
                        });
                    } else {
                        cache.setex(cbflag, 30, JSON.stringify({
                            code: 200,
                            data: coid
                        }), (err, result) => {
                            done();
                        });
                    }
                }).catch(e => {
                    log.info(e);
                });
            }).catch(e => {
                log.info(e);
                errorDone(cache, done, cbflag, e);
            });
        }).catch(e => {
            log.info(e);
            errorDone(cache, done, cbflag, e);
        });
    }).catch(e => {
        log.info(e);
        errorDone(cache, done, cbflag, e);
    });
});

function errorDone(cache: RedisClient, done: DoneFunction, cbflag: string, e) {
    cache.setex(cbflag, 30, JSON.stringify({
        code: 500,
        msg: e.message
    }), (err, result) => {
        done();
    });
    log.info(e);
}
processor.run();
console.log("Start processor at " + config.addr);
