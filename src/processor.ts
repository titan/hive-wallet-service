"use strict";
import { Processor, Config, ModuleFunction, DoneFunction, rpc, async_serial, async_serial_ignore } from "hive-processor";
import { Client as PGClient, ResultSet } from "pg";
import { createClient, RedisClient } from "redis";
import { servermap, triggermap } from "hive-hostmap";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import * as msgpack from "msgpack-lite";
import * as nanomsg from "nanomsg";
import {CustomerMessage} from "recommend-library";

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
function getLocalTime(nS) {
    return new Date(parseInt(nS) * 1000).toLocaleString().replace(/:\d{1,2}$/, " ");
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
            db.query("UPDATE accounts SET balance0 = balance0 + $1,balance1 = balance1 + $2 WHERE id = $3", [balance0, balance1, vid], (err: Error, result: ResultSet) => {
                if (err) {
                    db.query("ROLLBACK", [], (err) => {
                        log.error(err, "insert into accounts error");
                        done();
                    });
                }
                else {
                    db.query("INSERT INTO transactions(id,aid,type,title,amount) VALUES($1,$2,$3,$4,$5)", [tid, vid, type1, title, balance], (err: Error, result: ResultSet) => {
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
        db.query("id, uid, type, vid, balance0, balance1, created_at, updated_at", [], (err: Error, result: ResultSet) => {
            if (err) {
                log.info(err);
                reject(err);
            } else {
                const allaccounts = [];
                const vids = [];
                // let account = { balance0: balance0, balance1: balance1, id: aid, type: type, vehicle: vehicle };
                for (let row of result.rows) {
                    let account = {
                        balance0: row.balance0,
                        balance1: row.balance1,
                        id: row.id,
                        uid: null,
                        type: row.type,
                        vehicle: null
                    }
                    vids.push(row.id);
                    allaccounts.push(account);
                }
                let wvs = vids.map(vid => rpc<Object>(domain, servermap["vehicle"], null, "getVehicle", vid));
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
                });
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
                    multi.hset("wallet-entities", new_wallet["uid"], JSON.stringify(new_wallet));
                }
                multi.exec((err, result) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
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
        //let transactions = { amount: balance, occurred_at: created_at1, aid: vid, id: uid, title: title, type: type1 }; 
        db.query("id, aid, type, title, amount, occurred_at", [], (err: Error, result: ResultSet) => {
            if (err) {
                log.info(err);
                reject(err);
            } else {
                const transactions = [];
                const vids = [];
                for (let row of result.rows) {
                    let transaction = {
                        id: null,
                        amount: row.amount,
                        occurred_at: row.occurred_at.toLocaleString,
                        aid: row.aid,
                        title: row.title,
                        type: row.type
                    }
                    vids.push(row.aid);
                    transactions.push(transaction);
                }
                let tvs = vids.map(vid => rpc<Object>(domain, servermap["vehicle"], null, "getVehicle", vid));
                async_serial_ignore<Object>(tvs, [], (vreps) => {
                    const vehicles = vreps.filter(v => v["code"] === 200).map(v => v["data"]);
                    for (const vehicle of vehicles) {
                        for (const transaction of transactions) {
                            if (vehicle["id"] === transaction["aid"]) {
                                transaction["id"] = vehicle["user_id"];
                            }
                        }
                    }
                });
                let multi = cache.multi();
                for (let transaction of transactions) {
                    multi.zadd("transactions-" + transaction["id"], (new Date().getTime()), JSON.stringify(transaction));
                }
                multi.exec((err, result) => {
                    if (err) {
                        log.info(err);
                        reject(err);
                    } else {
                        resolve();
                    }
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


processor.run();
console.log("Start processor at " + config.addr);
