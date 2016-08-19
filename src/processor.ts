import * as nanomsg from 'nanomsg';
import * as Pool from 'pg-pool';
import * as pg from 'pg';
import * as msgpack from 'msgpack-lite';
import * as Redis from 'redis';

let addr = "ipc:///tmp/queue.ipc";
let pull = nanomsg.socket('pull');
pull.connect(addr);

var config = {
  host: process.env['DB_HOST'],
  user: process.env['DB_USER'],
  database: process.env['DB_NAME'],
  password: process.env['DB_PASSWORD'],
  port: 5432,
  min: 1, // min number of clients in the pool
  max: 2, // max number of clients in the pool
  idleTimeoutMillis: 30000, // how long a client is allowed to remain idle before being closed
};

let pool = new Pool(config);

pull.on('data', (input: any) => {
  let buf = msgpack.decode(input);
  if (buf.cmd == 'refresh') {
    pool.connect().then(client => {
      client.query('SELECT id, title, description, image, thumbnail, period FROM plans', [], (err: Error, result: pg.ResultSet) => {
        if (err) {
          console.error('query error', err.message, err.stack);
          return;
        }
        let plans = [];
        for (let row of result.rows) {
          plans.push(row2plan(row));
        }
        let countdown = plans.length; // indicate how many async callings are running
        for (let plan of plans) {
          client.query('SELECT id, name, title, description FROM plan_rules WHERE pid = $1', [ plan.id ], (err1: Error, result1: pg.ResultSet) => {
            countdown -= 1; 
            if (err1) {
              console.error('query error', err1.message, err1.stack);
            } else {
              for (let row of result1.rows) {
                plan.rules.push(row2rule(row));
              }
            }
            if (countdown == 0) {
              // all query are done
              client.end();
              let redis = Redis.createClient(6379, process.env['CACHE_HOST']); // port, host
              let multi = redis.multi();
              for (let plan of plans) {
                multi.hset("plan-entities", plan.id, JSON.stringify(plan));
              }
              for (let plan of plans) {
                multi.sadd("plans", plan.id);
              }
              multi.exec((err, replies) => {
                if (err) {
                  console.error(err);
                }
                redis.quit();
              });
            }
          });
        }
      });
    });
  }
});

pool.on('error', function (err, client) {
  console.error('idle client error', err.message, err.stack)
})

function row2plan(row) {
  return {
    id: row.id,
    title: row.title? row.title.trim(): '',
    description: row.description,
    image: row.image? row.image.trim(): '',
    thumbnail: row.thumbnail? row.thumbnail.trim(): '',
    period: row.period,
    rules: []
  };
}

function row2rule(row) {
  return {
    id: row.id,
    name: row.name? row.name.trim(): '',
    title: row.title? row.name.trim(): '',
    description: row.description
  };
}

console.log('Start processor at ' + addr);
