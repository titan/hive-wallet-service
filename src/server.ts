import { Service, Config, Context, CallbackFunction, Permission } from 'hive-service';
import * as Redis from "redis";

let redis = Redis.createClient();

let list_key = "plans";
let entities_prefix = "plans-";
let items_prefix = "plan-items-";
let entity_key = "plan-entities";
let item_key = "plan-items";

let config: Config = {
  svraddr: 'tcp://0.0.0.0:4040',
};

let svc = new Service(config);

let permissions: Permission[] = [['mobile', true], ['admin', true]];

svc.call('getAvailablePlans', permissions, (ctx: Context, cb: CallbackFunction) => {
  // http://redis.io/commands/sdiff
  redis.sdiff(list_key, entities_prefix + ctx.uid, function (err, result) {
    if (err) {
      cb([]);
    } else {
      ids2objects(entity_key, result, cb);
    }
  });
});

svc.call('getJoinedPlans', permissions, (ctx: Context, cb: CallbackFunction) => {
  // http://redis.io/commands/smembers
  redis.smembers(entities_prefix + ctx.uid, function (err, result) {
    if (err) {
      cb([]);
    } else {
      ids2objects(entity_key, result, cb);
    }
  });
});

svc.call('getPlanItems', permissions, (ctx: Context, cb: CallbackFunction, pid: string) => {
  // http://redis.io/commands/lrange
  redis.lrange(items_prefix + pid, 0, -1, function (err, result) {
    if (err) {
      cb([]);
    } else {
      ids2objects(item_key, result, cb);
    }
  });
});

function ids2objects(key: string, ids: string[], cb: CallbackFunction) {
  let multi = redis.multi();
  for (let id in ids) {
    multi.hget(key, id);
  }
  multi.exec(function(err, replies) {
    cb(replies);
  });
}

console.log('Start service at ' + config.svraddr);

svc.run();
