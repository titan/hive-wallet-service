"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments)).next());
    });
};
const hive_server_1 = require("hive-server");
;
const msgpack = require("msgpack-lite");
const bunyan = require("bunyan");
const hive_hostmap_1 = require("hive-hostmap");
const uuid = require("node-uuid");
const hive_verify_1 = require("hive-verify");
const bluebird = require("bluebird");
let log = bunyan.createLogger({
    name: "wallet-server",
    streams: [
        {
            level: "info",
            path: "/var/log/wallet-server-info.log",
            type: "rotating-file",
            period: "1d",
            count: 7
        },
        {
            level: "error",
            path: "/var/log/wallet-server-error.log",
            type: "rotating-file",
            period: "1w",
            count: 3
        }
    ]
});
let wallet_entities = "wallet-entities";
let transactions = "transactions-";
let config = {
    svraddr: hive_hostmap_1.servermap["wallet"],
    msgaddr: "ipc:///tmp/wallet.ipc",
    cacheaddr: process.env["CACHE_HOST"]
};
let svc = new hive_server_1.Server(config);
let permissions = [["mobile", true], ["admin", true]];
svc.call("createAccount", permissions, (ctx, rep, uid, type, vid, balance0, balance1) => {
    let aid = vid;
    let domain = ctx.domain;
    let args = { domain, uid, aid, type, vid, balance0, balance1 };
    log.info("createAccount", args);
    ctx.msgqueue.send(msgpack.encode({ cmd: "createAccount", args: [domain, uid, aid, type, vid, balance0, balance1] }));
    rep({ status: "200", data: aid });
});
svc.call("getWallet", permissions, (ctx, rep) => {
    log.info("getwallet" + ctx.uid);
    if (!hive_verify_1.verify([hive_verify_1.uuidVerifier("uid", ctx.uid)], (errors) => {
        rep({
            code: 400,
            msg: errors.join("\n")
        });
    })) {
        return;
    }
    ctx.cache.hget(wallet_entities, ctx.uid, function (err, result) {
        if (err || result === "" || result === null) {
            log.info("get redis error in getwallet");
            log.info(err);
            rep({ code: 404, msg: "walletinfo not found for this uid" });
        }
        else {
            let sum = null;
            let accounts = msgpack.decode(result);
            log.info(accounts);
            for (let account of accounts) {
                let balance = account.balance0 * 100 + account.balance1 * 100;
                sum += balance;
            }
            log.info("replies==========" + result);
            let result1 = { accounts: accounts, balance: sum / 100, id: ctx.uid };
            rep({ code: 200, data: result1 });
        }
    });
});
svc.call("getTransactions", permissions, (ctx, rep, offset, limit) => {
    log.info("getTransactions=====================");
    if (!hive_verify_1.verify([hive_verify_1.uuidVerifier("uid", ctx.uid)], (errors) => {
        rep({
            code: 400,
            msg: errors.join("\n")
        });
    })) {
        return;
    }
    ctx.cache.zrevrange(transactions + ctx.uid, offset, limit, function (err, result) {
        if (err || result === null || result == "") {
            log.info("get redis error in getTransactions");
            log.info(err);
            rep({ code: 500, msg: "未找到交易日志" });
        }
        else {
            const alltransactions = [];
            for (let t of result) {
                if (t !== null) {
                    const transaction = msgpack.decode(t);
                    alltransactions.push(transaction);
                }
            }
            rep({ code: 200, data: alltransactions });
        }
    });
});
svc.call("updateAccountbalance", permissions, (ctx, rep, uid, vid, type1, balance0, balance1) => {
    log.info("getTransactions=====================");
    let domain = ctx.domain;
    let args = { domain, uid, vid, type1, balance0, balance1 };
    log.info("createAccount", args);
    ctx.msgqueue.send(msgpack.encode({ cmd: "updateAccountbalance", args: [domain, uid, vid, type1, balance0, balance1] }));
    rep({ code: 200, data: "200" });
});
svc.call("refresh", permissions, (ctx, rep) => {
    ctx.msgqueue.send(msgpack.encode({ cmd: "refresh", args: [] }));
    rep({ code: 200, data: "200" });
});
svc.call("applyCashOut", permissions, (ctx, rep, order_id) => {
    log.info("applyCashOut uuid is " + ctx.uid);
    let user_id = ctx.uid;
    if (!hive_verify_1.verify([hive_verify_1.uuidVerifier("order_id", order_id), hive_verify_1.uuidVerifier("user_id", user_id)], (errors) => {
        log.info(errors);
        rep({
            code: 400,
            msg: errors.join("\n")
        });
    })) {
        return;
    }
    let callback = uuid.v1();
    let domain = ctx.domain;
    ctx.msgqueue.send(msgpack.encode({ cmd: "applyCashOut", args: [domain, order_id, user_id, callback] }));
    hive_server_1.wait_for_response(ctx.cache, callback, rep);
});
svc.call("agreeCashOut", permissions, (ctx, rep, coid, state, user_id, opid) => {
    log.info("agreeCashOut uuid is " + ctx.uid);
    if (!hive_verify_1.verify([hive_verify_1.uuidVerifier("coid", coid), hive_verify_1.uuidVerifier("user_id", user_id)], (errors) => {
        log.info(errors);
        rep({
            code: 400,
            msg: errors.join("\n")
        });
    })) {
        return;
    }
    let callback = uuid.v1();
    let domain = ctx.domain;
    ctx.msgqueue.send(msgpack.encode({ cmd: "agreeCashOut", args: [domain, coid, state, opid, user_id, callback] }));
    hive_server_1.wait_for_response(ctx.cache, callback, rep);
});
svc.call("getAppliedCashouts", permissions, (ctx, rep) => {
    log.info("getAppliedCashouts");
    (() => __awaiter(this, void 0, void 0, function* () {
        try {
            const keys = yield ctx.cache.zrevrangeAsync("applied-cashouts", 0, -1);
            let multi = bluebird.promisifyAll(ctx.cache.multi());
            for (let key of keys) {
                multi.hget("cashout-entities", key);
            }
            const cashouts = yield multi.execAsync();
            rep({ code: 200, data: cashouts.map(e => JSON.parse(e)) });
        }
        catch (e) {
            log.error(e);
            rep({ code: 500, msg: e.message });
        }
    }))();
});
svc.call("getAppliedCashouts", permissions, (ctx, rep) => {
    log.info("getAppliedCashouts");
    (() => __awaiter(this, void 0, void 0, function* () {
        try {
            const keys = yield ctx.cache.zrevrangeAsync("applied-cashouts", 0, -1);
            let multi = bluebird.promisifyAll(ctx.cache.multi());
            for (let key of keys) {
                multi.hget("cashout-entities", key);
            }
            const cashouts = yield multi.execAsync();
            rep({ code: 200, data: cashouts.map(e => JSON.parse(e)) });
        }
        catch (e) {
            log.error(e);
            rep({ code: 500, msg: e.message });
        }
    }))();
});
svc.call("getAgreedCashouts", permissions, (ctx, rep) => {
    log.info("getAgreedCashouts");
    (() => __awaiter(this, void 0, void 0, function* () {
        try {
            const keys = yield ctx.cache.zrevrangeAsync("agreed-cashouts", 0, -1);
            let multi = bluebird.promisifyAll(ctx.cache.multi());
            for (let key of keys) {
                multi.hget("cashout-entities", key);
            }
            const cashouts = yield multi.execAsync();
            rep({ code: 200, data: cashouts.map(e => JSON.parse(e)) });
        }
        catch (e) {
            log.error(e);
            rep({ code: 500, msg: e.message });
        }
    }))();
});
svc.call("getRefusedCashouts", permissions, (ctx, rep) => {
    log.info("getRefusedCashouts");
    (() => __awaiter(this, void 0, void 0, function* () {
        try {
            const keys = yield ctx.cache.zrevrangeAsync("refused-cashouts", 0, -1);
            let multi = bluebird.promisifyAll(ctx.cache.multi());
            for (let key of keys) {
                multi.hget("cashout-entities", key);
            }
            const cashouts = yield multi.execAsync();
            rep({ code: 200, data: cashouts.map(e => JSON.parse(e)) });
        }
        catch (e) {
            log.error(e);
            rep({ code: 500, msg: e.message });
        }
    }))();
});
console.log("Start service at " + config.svraddr);
svc.run();
