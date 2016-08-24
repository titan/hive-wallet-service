import { Service, Config, Context, ResponseFunction, Permission } from 'hive-service';
import * as Redis from "redis";
import * as nanomsg from 'nanomsg';
import * as msgpack from 'msgpack-lite';

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
  redis.lrange(wallet_entity + ctx.uid, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2objects(wallet_key, result, rep);
    }
  });
});

svc.call('getAccounts', permissions, (ctx: Context, rep: ResponseFunction) => {
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
  // http://redis.io/commands/lrange
  redis.lrange(transactions_entities + ctx.uid, 0, -1, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2objects(transactions_key, result, rep);
    }
  });
});

svc.call('refresh', permissions, (ctx: Context, rep: ResponseFunction) => {
  console.log('111')
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
