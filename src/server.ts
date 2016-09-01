import { Service, Config, Context, ResponseFunction, Permission } from 'hive-service';
import * as Redis from "redis";
import * as nanomsg from 'nanomsg';
import * as msgpack from 'msgpack-lite';
import * as bunyan from 'bunyan';

let log = bunyan.createLogger({
  name: 'wallet-server',
  streams: [
    {
      level: 'info',
      path: '/var/log/server-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/server-error.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1w',   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let redis = Redis.createClient(6379, "redis"); // port, host

let wallet_entity = "wallets-";
let account_entities = "accounts-";
let transactions_entities = "transactions-";
let list_key = "wallets"
let wallet_key = "wallet";
let account_key = "accounts";
let transactions_key = "transactions";
let config: Config = {
  svraddr: 'tcp://0.0.0.0:4040',
  msgaddr: 'ipc:///tmp/queue.ipc'
};

let svc = new Service(config);

let permissions: Permission[] = [['mobile', true], ['admin', true]];

svc.call('getWallet', permissions, (ctx: Context, rep: ResponseFunction) => {
  // http://redis.io/commands/sdiff
  log.info('getWallet %j', ctx);
  redis.lrange(wallet_entity + ctx.uid, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2objects(wallet_key, result, rep);
    }
  });
});

svc.call('getAccounts', permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info('getAccounts %j', ctx);
  // http://redis.io/commands/smembers
  redis.smembers(account_entities + ctx.uid,0,-1, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2objects(account_key, result, rep);
    }
  });
});

svc.call('getTransactions', permissions, (ctx: Context, rep: ResponseFunction, pid: string) => {
  log.info('getTransactions %j', ctx);
  // http://redis.io/commands/lrange
  redis.lrange(transactions_entities + ctx.uid, 0, -1, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2objects(transactions_key, result, rep);
    }
  });
});

svc.call('wallet', permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info('wallet %j', ctx);
  ctx.msgqueue.send(msgpack.encode({cmd: "refresh", args: null}));
});

function ids2objects(key: string, ids: string[], rep: ResponseFunction) {
  let multi = redis.multi();
  for (let id of ids) {
    multi.hget(key, id);
  }
  multi.exec(function(err, replies) {
    rep(replies);
  });
}

console.log('Start service at ' + config.svraddr);

svc.run();
