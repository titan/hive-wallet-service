"use strict";
const hive_server_1 = require('hive-server');
const Redis = require("redis");
const msgpack = require('msgpack-lite');
const bunyan = require('bunyan');
const hostmap = require('./hostmap');
const uuid = require('uuid');
let log = bunyan.createLogger({
    name: 'wallet-server',
    streams: [
        {
            level: 'info',
            path: '/var/log/wallet-server-info.log',
            type: 'rotating-file',
            period: '1d',
            count: 7
        },
        {
            level: 'error',
            path: '/var/log/wallet-server-error.log',
            type: 'rotating-file',
            period: '1w',
            count: 3
        }
    ]
});
let redis = Redis.createClient(6379, "redis");
let wallet_entities = "wallet-entities";
let transactions = "transactions-";
let config = {
    svraddr: hostmap.default["wallet"],
    msgaddr: 'ipc:///tmp/wallet.ipc'
};
let svc = new hive_server_1.Server(config);
let permissions = [['mobile', true], ['admin', true]];
svc.call('createAccount', permissions, (ctx, rep, uid = type, string, vid, balance0, balance1) => {
    let uid = ctx.uid;
    let aid = uuid.v1();
    let args = { ctx: ctx, uid: uid, aid: aid, type: type, vid: vid, balance0: balance0, balance1: balance1 };
    log.info('createAccount', args);
    ctx.msgqueue.send(msgpack.encode({ cmd: "createAccount", args: args }));
    rep({ status: "200", aid: aid });
});
svc.call('getWallet', permissions, (ctx, rep) => {
    log.info('getwallet');
    redis.hget(wallet_entities + ctx.uid, function (err, result) {
        if (err) {
            log.info('get redis error in getwallet');
            log.info(err);
            rep({ code: 500, msg: "walletinfo not found for this uid" });
        }
        else {
            let sum = 0;
            let accounts = JSON.parse(result);
            for (let account of accounts) {
                let balance = account.balance0 + account.balance1;
                sum += balance;
            }
            log.info('replies==========' + result);
            let result1 = { accounts: JSON.parse(result), balance: sum, id: ctx.uid };
            rep({ code: 200, wallet: result1 });
        }
    });
});
svc.call('getTransactions', permissions, (ctx, rep, offset, limit) => {
    log.info('getTransactions=====================');
    redis.zrevrange(transactions + ctx.uid, offset, limit, function (err, result) {
        if (err) {
            log.info('get redis error in getTransactions');
            log.info(err);
            rep({ code: 500, msg: "未找到交易日志" });
        }
        else {
            rep(JSON.parse(result));
        }
    });
});
console.log('Start service at ' + config.svraddr);
svc.run();
