import * as nano from 'nanomsg';
import * as msgpack from 'msgpack-lite';
import * as Redis from "redis";

interface Context {
  domain: string,
  ip: string,
  uid: string
}

interface CallbackFunction {
  (result: any): void;
}

interface ModuleFunction {
  (ctx: Context, cb: CallbackFunction, ...rest: any[]): void;
}

let redis = Redis.createClient();

let list_key = "plans";
let entities_prefix = "plans-";
let items_prefix = "plan-items-";
let entity_key = "plan-entities";
let item_key = "plan-items";

let rep = nano.socket('rep');
let push = nano.socket('push');

let repaddr = 'tcp://0.0.0.0:4040'; // 对外服务地址
let pushaddr = 'ipc:///tmp/pipeline.ipc'; // processor 地址

let getAvailablePlans: ModuleFunction;
getAvailablePlans = function (ctx: Context, cb: CallbackFunction) {
  // http://redis.io/commands/sdiff
  redis.sdiff(list_key, entities_prefix + ctx.uid, function (err, result) {
    if (err) {
      cb(msgpack.encode([]));
    } else {
      ids2objects(entity_key, result, cb);
    }
  });
}

let getJoinedPlans: ModuleFunction;
getJoinedPlans = function (ctx: Context, cb: CallbackFunction) {
  // http://redis.io/commands/smembers
  redis.smembers(entities_prefix + ctx.uid, function (err, result) {
    if (err) {
      cb(msgpack.encode([]));
    } else {
      ids2objects(entity_key, result, cb);
    }
  });
}

let getPlanItems: ModuleFunction;
getPlanItems = function (ctx: Context, cb: CallbackFunction, pid: string) {
  // http://redis.io/commands/lrange
  redis.lrange(items_prefix + pid, 0, -1, function (err, result) {
    if (err) {
      cb(msgpack.encode([]));
    } else {
      ids2objects(item_key, result, cb);
    }
  });
}

let functions: ModuleFunction[] = [getAvailablePlans, getJoinedPlans, getPlanItems];

let funmap: Map<string, number> = new Map<string, number>([
  ['getAvailablePlans', 0],
  ['getJoionedPlans', 1],
  ['getPlanItems', 2]
]);

let dommap: Map<string, number> = new Map<string, number>([
  ['mobile', 0],
  ['admin', 1]
]);

let permissions: boolean[][] = [
  [true, true, true],
  [true, true, true]
];

rep.bind(repaddr);
//push.bind(pushaddr);

rep.on('data', function (buf: NodeBuffer) {
  let pkt = msgpack.decode(buf);
  let ctx: Context = pkt.ctx; /* Domain, IP, User */
  let fun = pkt.fun;
  let args = pkt.args;
  if (dommap.has(ctx.domain) && funmap.has(fun) && permissions[dommap.get(ctx.domain)][funmap.get(fun)]) {
    let func: ModuleFunction = functions[funmap.get(fun)];
    func(ctx, function(result) {
      rep.send(result);
    }, args);
  } else {
    rep.send(msgpack.encode({code: 403, msg: "Forbidden"}));
  }
});

function ids2objects(key, ids, cb) {
  let multi = redis.multi();
  for (let id in ids) {
    multi.hget(key, id);
  }
  multi.exec(function(err, replies) {
    cb(msgpack.encode(replies));
  });
}
