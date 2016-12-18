"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments)).next());
    });
};
const hive_processor_1 = require("hive-processor");
const hive_hostmap_1 = require("hive-hostmap");
const bunyan = require("bunyan");
const uuid = require("node-uuid");
const msgpack = require("msgpack-lite");
const UUID = require("uuid-1345");
const queryString = require("querystring");
const http = require("http");
const bluebird = require("bluebird");
let log = bunyan.createLogger({
    name: "wallet-processor",
    streams: [
        {
            level: "info",
            path: "/var/log/wallet-processor-info.log",
            type: "rotating-file",
            period: "1d",
            count: 7
        },
        {
            level: "error",
            path: "/var/log/wallet-processor-error.log",
            type: "rotating-file",
            period: "1w",
            count: 3
        }
    ]
});
let config = {
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
function trim(str) {
    if (str) {
        return str.trim();
    }
    else {
        return null;
    }
}
let processor = new hive_processor_1.Processor(config);
processor.call("createAccount", (db, cache, done, domain, uid, aid, type, vid, balance0, balance1) => {
    log.info("createAccount");
    let balance = balance0 + balance1;
    let tid = uuid.v1();
    let title = `参加计划 收入`;
    let created_at = new Date().getTime();
    let created_at1 = getLocalTime(created_at / 1000);
    db.query("BEGIN", (err) => {
        if (err) {
            log.error(err, "query error");
            done();
        }
        else {
            db.query("INSERT INTO accounts(id,uid,type,vid,balance0,balance1) VALUES($1,$2,$3,$4,$5,$6)", [aid, uid, type, vid, balance0, balance1], (err) => {
                if (err) {
                    db.query("ROLLBACK", [], (err) => {
                        log.error(err, "insert into accounts error");
                        done();
                    });
                }
                else {
                    db.query("INSERT INTO transactions(id,aid,type,title,amount) VALUES($1,$2,$3,$4,$5)", [tid, aid, type, title, balance], (err) => {
                        if (err) {
                            db.query("ROLLBACK", [], (err) => {
                                log.error(err, "insert into transactions error");
                                done();
                            });
                        }
                        else {
                            db.query("COMMIT", [], (err) => {
                                if (err) {
                                    log.info(err);
                                    log.error(err, "insert plan order commit error");
                                    done();
                                }
                                else {
                                    let p = hive_processor_1.rpc(domain, hive_hostmap_1.servermap["vehicle"], null, "getVehicle", vid);
                                    p.then((v) => {
                                        if (err) {
                                            done();
                                            log.info("call vehicle error");
                                        }
                                        else {
                                            cache.hget("wallet-entities", uid, function (err, result2) {
                                                if (err) {
                                                    log.info("get wallete-entities err");
                                                    done();
                                                }
                                                else if (result2 === "" || result2 === null) {
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
                                                        }
                                                        else {
                                                            log.info("placeAnDriverOrder:==========is done");
                                                            done();
                                                        }
                                                    });
                                                }
                                                else {
                                                    let accounts = msgpack.decode(result2);
                                                    let vehicle = v["data"];
                                                    let multi = cache.multi();
                                                    let transactions = { amount: balance, occurred_at: created_at1, aid: aid, id: uid, title: title, type: 1 };
                                                    let account = { balance0: balance0, balance1: balance1, id: aid, type: type, vehicle: vehicle };
                                                    accounts.push(account);
                                                    multi.zadd("transactions-" + uid, created_at, msgpack.encode(transactions));
                                                    multi.hset("wallet-entities", uid, msgpack.encode(accounts));
                                                    multi.exec((err3, replies) => {
                                                        if (err3) {
                                                            log.error(err3, "query redis error");
                                                            done();
                                                        }
                                                        else {
                                                            log.info("==========is done");
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
        }
    });
});
processor.call("updateAccountbalance", (db, cache, done, domain, uid, vid, type1, balance0, balance1) => {
    log.info("updateOrderState");
    let created_at = new Date().getTime();
    let created_at1 = getLocalTime(created_at / 1000);
    let balance = balance0 + balance1;
    let title = `添加司机`;
    let tid = uuid.v1();
    db.query("BEGIN", (err) => {
        if (err) {
            log.error(err, "query error");
            done();
        }
        else {
            db.query("UPDATE accounts SET balance0 = balance0 + $1,balance1 = balance1 + $2 WHERE id = $3", [balance0, balance1, vid], (err, result) => {
                if (err) {
                    db.query("ROLLBACK", [], (err) => {
                        log.error(err, "insert into accounts error");
                        done();
                    });
                }
                else {
                    db.query("INSERT INTO transactions(id,aid,type,title,amount) VALUES($1,$2,$3,$4,$5)", [tid, vid, type1, title, balance], (err, result) => {
                        if (err) {
                            db.query("ROLLBACK", [], (err) => {
                                log.error(err, "insert into accounts error");
                                done();
                            });
                        }
                        else {
                            db.query("COMMIT", [], (err) => {
                                if (err) {
                                    log.info(err);
                                    log.error(err, "insert plan order commit error");
                                    done();
                                }
                                else {
                                    let multi = cache.multi();
                                    multi.hget("wallet-entities", uid, function (err, replise) {
                                        if (err) {
                                            log.info("err,get redis error");
                                            done();
                                        }
                                        else {
                                            let accounts = msgpack.decode(replise);
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
                                            multi.hset("wallet-entities", uid, msgpack.encode(accounts));
                                            multi.zadd("transactions-" + uid, created_at, msgpack.encode(transactions));
                                            multi.exec((err, result1) => {
                                                if (err) {
                                                    log.info("err:hset order_entities error");
                                                    done();
                                                }
                                                else {
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
function refresh_wallets(db, cache, done, domain) {
    return sync_wallets(db, cache, done, domain);
}
function sync_wallets(db, cache, done, domain) {
    return new Promise((resolve, reject) => {
        db.query("SELECT id, uid, type, vid, balance0, balance1,created_at, updated_at FROM accounts WHERE deleted = false", [], (err, result) => {
            if (err) {
                log.info(err);
                reject(err);
            }
            else {
                const allaccounts = [];
                const vids = [];
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
                let wvs = vids.map(vid => hive_processor_1.rpc("mobile", hive_hostmap_1.servermap["vehicle"], null, "getVehicle", vid));
                hive_processor_1.async_serial_ignore(wvs, [], (vreps) => {
                    const vehicles = vreps.filter(v => v["code"] === 200).map(v => v["data"]);
                    for (const vehicle of vehicles) {
                        for (const account of allaccounts) {
                            if (vehicle["id"] === account["id"]) {
                                account["vehicle"] = vehicle;
                                account["uid"] = vehicle["user_id"];
                            }
                        }
                    }
                    let wallets = {};
                    const new_wallets = [];
                    wallets = allaccounts.reduce((acc, account) => {
                        const uid = account["uid"];
                        if (acc[uid]) {
                            acc[uid].push(account);
                        }
                        else {
                            acc[uid] = [account];
                        }
                        return acc;
                    }, {});
                    for (let wallet in wallets) {
                        new_wallets.push(wallets[wallet]);
                    }
                    let multi = cache.multi();
                    for (let new_wallet of new_wallets) {
                        multi.hset("wallet-entities", new_wallet[0]["uid"], msgpack.encode(new_wallet));
                    }
                    multi.exec((err, result) => {
                        if (err) {
                            reject(err);
                        }
                        else {
                            resolve();
                        }
                    });
                });
            }
        });
    });
}
function refresh_transitions(db, cache, done, domain) {
    return sync_transitions(db, cache, done, domain);
}
function sync_transitions(db, cache, done, domain) {
    return new Promise((resolve, reject) => {
        db.query("SELECT aid, type, title, amount, occurred_at FROM transactions", [], (err, result) => {
            if (err) {
                log.info(err);
                reject(err);
            }
            else {
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
                let tvs = vids.map(vid => hive_processor_1.rpc("mobile", hive_hostmap_1.servermap["vehicle"], null, "getVehicle", vid));
                hive_processor_1.async_serial_ignore(tvs, [], (vreps) => {
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
                        multi.zadd("transactions-" + transaction["id"], new Date(transaction["occurred_at"]), msgpack.encode(transaction));
                    }
                    multi.exec((err, result) => {
                        if (err) {
                            log.info(err);
                            reject(err);
                        }
                        else {
                            resolve();
                        }
                    });
                });
            }
        });
    });
}
processor.call("refresh", (db, cache, done, domain) => {
    const RW = refresh_wallets(db, cache, done, domain);
    const RT = refresh_transitions(db, cache, done, domain);
    let ps = [RW, RT];
    hive_processor_1.async_serial_ignore(ps, [], () => {
        log.info("refresh done!");
        done();
    });
});
processor.call("applyCashOut", (db, cache1, done, domain, order_id, user_id, cbflag) => {
    log.info("applyCashOut");
    const cache = bluebird.promisifyAll(cache1);
    let date = new Date();
    let year = date.getFullYear();
    let month = (date.getMonth() + 1) < 10 ? "0" + (date.getMonth() + 1) : (date.getMonth() + 1);
    let day = date.getDate() < 10 ? "0" + date.getDate() : date.getDate();
    let dateString = year + "" + month + "" + day;
    let cash_no = dateString;
    let state = 0;
    let coid = uuid.v1();
    (() => __awaiter(this, void 0, void 0, function* () {
        try {
            const cash_counter = yield cache.hincrbyAsync("cashout-counter", dateString, 1);
            for (let i = 0, len = 4 - cash_counter.toString().length; i < len; i++) {
                cash_no += "0";
            }
            cash_no += cash_counter;
            const accountsjson = yield cache.hgetAsync("wallet-entities", user_id);
            const vid = yield cache.hgetAsync("orderid-vid", order_id);
            const accounts = JSON.parse(accountsjson);
            const amount = accounts.filter(x => x["vehicle"]["id"] === vid).reduce((acc, x) => x["balance0"] + x["balance1"], 0);
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
            let multi = bluebird.promisifyAll(cache.multi());
            multi.hset("cashout-entities", coid, JSON.stringify(cashout_entity));
            multi.zadd("applied-cashouts", date.getTime(), coid);
            yield multi.execAsync();
            const myuuid = yield UUID.v3({ namespace: UUID.namespace.url, name: cashout_entity["no"] + cashout_entity["updated_at"] + cashout_entity["state"].toString() });
            yield db.query("INSERT INTO cashout(id, no, state, amount, order_id, last_event_id) VALUES($1, $2, $3, $4, $5, $6)", [coid, cash_no, state, amount, order_id, myuuid]);
            yield db.query("INSERT INTO cashout_events(id, type, opid, uid, data) VALUES ($1, $2, $3, $4, $5)", [myuuid, cashout_entity["state"], user_id, user_id, cashout_entity]);
            yield cache.setexAsync(cbflag, 30, JSON.stringify({ code: 200, data: coid }));
            done();
        }
        catch (e) {
            log.error(e);
            yield cache.setexAsync(cbflag, 30, JSON.stringify({ code: 500, msg: e.message }));
            done();
        }
    }))();
});
processor.call("agreeCashOut", (db, cache, done, domain, coid, state, opid, user_id, cbflag) => {
    log.info("agreeCashOut");
    (() => __awaiter(this, void 0, void 0, function* () {
        try {
            const date = new Date();
            const cashoutjson = yield cache.hgetAsync("cashout-entities", coid);
            let cashout = JSON.parse(cashoutjson);
            cashout["state"] = state;
            cashout["updated_at"] = date;
            const cashout_entity = cashout;
            const myuuid = yield UUID.v3({ namespace: UUID.namespace.url, name: cashout_entity["no"] + cashout_entity["updated_at"] + cashout_entity["state"].toString() });
            yield db.query("UPDATE cashout SET state = $1 , updated_at = $2, last_event_id = $3 WHERE id = $4 AND deleted = false", [state, date, myuuid, coid]);
            let multi = bluebird.promisifyAll(cache.multi());
            multi.hset("cashout-entities", coid, JSON.stringify(cashout));
            multi.zrem("applied-cashouts", coid);
            if (state === 1) {
                multi.zadd("agreed-cashouts", date.getTime(), coid);
            }
            if (state === 2) {
                multi.zadd("refused-cashouts", date.getTime(), coid);
            }
            yield multi.execAsync();
            yield db.query("INSERT INTO cashout_events(id, type, opid, uid, data) VALUES ($1, $2, $3, $4, $5)", [myuuid, cashout_entity["state"], opid, user_id, cashout_entity]);
            const orderjson = yield cache.hgetAsync("order-entities", cashout_entity["order_id"]);
            const order_no = JSON.parse(orderjson)["id"];
            if (state === 1) {
                const test = process.env["WX_ENV"] === "test" ? true : false;
                const profile = yield hive_processor_1.rpc(domain, hive_hostmap_1.servermap["profile"], null, "getUserByUserId", user_id);
                const payment = yield hive_processor_1.rpc(domain, hive_hostmap_1.servermap["bank_payment"], null, "getCustomerId", profile["data"]["pnrid"]);
                const bank_amount = cashout_entity["amount"].toFixed(2).toString();
                const generate = yield hive_processor_1.rpc(domain, hive_hostmap_1.servermap["bank_payment"], null, "generateCashUrl", order_no, payment["cid"], bank_amount, test);
                const url = generate["url"];
                const order = yield hive_processor_1.rpc(domain, hive_hostmap_1.servermap["order"], null, "updateOrderState", user_id, order_no, 6, "待退款");
                const postData = queryString.stringify({ "user": profile["data"]["openid"], "amount": cashout_entity["amount"], "url": url });
                const options = {
                    hostname: wxhost, port: 80, path: "/wx/wxpay/tmsgCashOut", method: "GET",
                    headers: {
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Content-Length": Buffer.byteLength(postData)
                    }
                };
                const req = yield http.request(options, (res) => {
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
                yield cache.setexAsync(cbflag, 30, JSON.stringify({ code: 200, data: coid }));
                done();
            }
            else {
                yield cache.setexAsync(cbflag, 30, JSON.stringify({ code: 200, data: coid }));
                done();
            }
        }
        catch (e) {
            log.error(e);
            yield cache.setexAsync(cbflag, 30, JSON.stringify({ code: 500, msg: e.message }));
            done();
        }
    }))();
});
processor.run();
console.log("Start processor at " + config.addr);
