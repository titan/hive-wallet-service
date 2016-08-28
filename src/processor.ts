"use strict";
import { Processor, Config, ModuleFunction, DoneFunction } from 'hive-processor';
import { Client as PGClient, ResultSet } from 'pg';
import { createClient, RedisClient} from 'redis';
import * as bunyan from 'bunyan';

let log = bunyan.createLogger({
  name: 'wallet-processor',
  streams: [
    {
      level: 'info',
      path: '/var/log/processor-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/processor-error.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1w',   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let config = {
    dbhost: process.env['DB_HOST'],
    dbuser: process.env['DB_USER'],
    database: process.env['DB_NAME'],
    dbpasswd: process.env['DB_PASSWORD'],
    cachehost: process.env['CACHE_HOST'],
    addr: "ipc:///tmp/queue.ipc"
};
let processor = new Processor(config);

processor.call('wallet', (db, cache, done) => {
    log.info('wallet');
    db.query('SELECT id, balance FROM wallets', [], (err, result) => {
        if (err) {
            log.error(err, 'query error');
            return;
        }
       
        let wallets = [];
        for (let row of result.rows) {
            wallets.push(row2wallet(row));
        }
        let multi = cache.multi();
        for (let wallet of wallets) {
            multi.hset("wallet_entity", wallet.id, JSON.stringify(wallet));
        }
        for (let wallet of wallets) {
            multi.sadd("wallets", wallet.id)
        }
        multi.exec((err1, replies) => {
            if (err1) {
            log.error(err1, 'query error');
            }
            done();
        });
    });
});
processor.call('accounts', (db, cache, done) => {
    log.info('accounts');
    db.query('SELECT id, type, vid, balance0, balance1 FROM accounts', [], (err2, result) => {
        if (err2) {
            log.error(err2, 'query error');
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
        multi.exec((err3, replies) => {
            if (err3) {
                log.error(err3, 'query error');
            }
            done();
        });
    });
});
processor.call('transaction', (db, cache, done) => {
    log.info('transaction');
    db.query('SELECT id, type, title, occurred_at, amount FROM transactions', [], (err4, result) => {
        if (err4) {
            log.error(err4, 'query error');
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
        multi.exec((err5, replies) => {
            if (err5) {
                log.error(err5, 'query error');
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
