"use strict";
const hive_processor_1 = require('hive-processor');
const bunyan = require('bunyan');
const hostmap = require('./hostmap');
const uuid = require('uuid');
let log = bunyan.createLogger({
    name: 'wallet-processor',
    streams: [
        {
            level: 'info',
            path: '/var/log/wallet-processor-info.log',
            type: 'rotating-file',
            period: '1d',
            count: 7
        },
        {
            level: 'error',
            path: '/var/log/wallet-processor-error.log',
            type: 'rotating-file',
            period: '1w',
            count: 3
        }
    ]
});
let config = {
    dbhost: process.env['DB_HOST'],
    dbuser: process.env['DB_USER'],
    dbport: process.env['DB_PORT'],
    database: process.env['DB_NAME'],
    dbpasswd: process.env['DB_PASSWORD'],
    cachehost: process.env['CACHE_HOST'],
    addr: "ipc:///tmp/wallet.ipc"
};
function getLocalTime(nS) {
    return new Date(parseInt(nS) * 1000).toLocaleString().replace(/:\d{1,2}$/, ' ');
}
let processor = new hive_processor_1.Processor(config);
processor.call('createAccount', (db, cache, done, args) => {
    log.info('createAccount');
    let balance = args.balance0 + args.balance1;
    let tid = uuid.v1();
    let title = `加入计划 充值 ${balance}元`;
    let created_at = new Date().getTime();
    let created_at1 = getLocalTime(created_at / 1000);
    db.query('BEGIN', (err) => {
        if (err) {
            log.error(err, 'query error');
            done();
        }
        else {
            db.query('INSERT INTO accounts(id,uid,type,vid,balance0,balance1) VALUES($1,$2,$3,$4,$5,$6)', [args.aid, args.uid, args.type, args.vid, args.balance0, args.balance1], (err) => {
                if (err) {
                    log.info(err + 'insert into accounts error in wallet');
                    done();
                    return;
                }
                else {
                    db.query('INSERT INTO transactions(id,aid,type,title,amount) VALUES($1,$2,$3,$4,$5)', [tid, args.aid, args.type, title, balance], (err) => {
                        if (err) {
                            log.info(err + 'insert into transactions error in wallet');
                            done();
                            return;
                        }
                        else {
                            db.query('COMMIT', [], (err) => {
                                if (err) {
                                    log.info(err);
                                    log.error(err, 'insert plan order commit error');
                                    done();
                                }
                                else {
                                    let p = hive_processor_1.rpc(args.ctx.domain, hostmap.default["vehicle"], null, "getModelAndVehicleInfo", args.vid);
                                    p.then((vehicle) => {
                                        if (err) {
                                            log.info("call vehicle error");
                                        }
                                        else {
                                            let multi = cache.multi();
                                            let transactions = { amount: balance, occurred_at: created_at1, aid: args.aid, id: args.uid, title: title, type: 1 };
                                            let accounts = { balance0: args.balance0, balance1: args.balance1, id: args.aid, type: args.type, vehicle: vehicle };
                                            multi.zadd("transactions-" + args.uid, created_at, JSON.stringify(transactions));
                                            multi.hset("wallet-entities", args.uid, JSON.stringify(accounts));
                                            multi.exec((err3, replies) => {
                                                if (err3) {
                                                    log.error(err3, 'query redis error');
                                                }
                                                else {
                                                    log.info('placeAnDriverOrder:==========is done');
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
processor.run();
console.log('Start processor at ' + config.addr);
