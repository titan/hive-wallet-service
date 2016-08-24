"use strict";
const hive_processor_1 = require('hive-processor');
let config = {
    dbhost: process.env['DB_HOST'],
    dbuser: process.env['DB_USER'],
    database: process.env['DB_NAME'],
    dbpasswd: process.env['DB_PASSWORD'],
    cachehost: process.env['CACHE_HOST'],
    addr: "ipc:///tmp/queue.ipc"
};
let processor = new hive_processor_1.Processor(config);
// console.log(config);
console.log('config');
processor.call('refresh', (db, cache, done) => {
    console.log('333');
    db.query('SELECT id, balance FROM wallets', [], (err, result) => {
        if (err) {
            console.error('query error', err.message, err.stack);
            return;
        }
       
        let wallets = [];
        for (let row of result.rows) {
            wallets.push(row2wallet(row));
        }
        let multi = cache.multi();
        for (let wallet of wallets) {
           console.log(wallet.id);
            multi.hset("wallet_entity", wallet.id, JSON.stringify(wallet));
        }
        for (let wallet of wallets) {
            multi.sadd("wallets", wallet.id)
        }
        multi.exec((err, replies) => {
            if (err) {
                console.error(err);
            }
            done();
        });
    });
});
processor.call('accounts', (db, cache, done) => {
    db.query('SELECT id, type, vid, balance0, balance1 FROM accounts', [], (err, result) => {
        if (err) {
            console.error('query error', err.message, err.stack);
            return;
        }
        let accounts = [];
        for (let row1 of result.rows) {
            accounts.push(row2account(row1));
        }
        let multi = cache.multi();
        for (let account of accounts) {
            multi.hset("account", account.id, JSON.stringify(account));
        }
        for (let account of accounts) {
            multi.sadd("accounts", account.id);
        }
        multi.exec((err, replies) => {
            if (err) {
                console.error(err);
            }
            done();
        });
    });
});
processor.call('transaction', (db, cache, done) => {
    db.query('SELECT id, type, title, occurred_at, amount FROM transactions', [], (err, result) => {
        if (err) {
            console.error('query error', err.message, err.stack);
            return;
        }
        let transactions = [];
        for (let row2 of result.rows) {
            transactions.push(row2transations(row2));
        }
        let multi = cache.multi();
        for (let transaction of transactions) {
            multi.hset("transaction", transaction.id, JSON.stringify(transaction));
        }
        for (let transaction of transactions) {
            multi.sadd("transactions", transaction.id);
        }
        multi.exec((err, replies) => {
            if (err) {
                console.error(err);
            }
            done();
        });
    });
});
function row2wallet(row) {
    return {
        id: row.id,
        balance: row.balance
    };
}
function row2account(row1) {
    return {
        id: row1.id,
        type: row1.type,
        vid: row1.vid,
        balance0: row1.balance0,
        balance1: row1.balance1
    };
}
function row2transations(row2) {
    return {
        id: row2.id,
        type: row2.type,
        title: row2.title,
        occurred_at: row2.occurred_at,
        amount: row2.amount
    };
}
processor.run();
console.log('Start processor at ' + config.addr);
