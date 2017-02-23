import { BusinessEventContext, BusinessEventHandlerFunction, BusinessEventListener, rpc, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, set_for_response, waiting, msgpack_decode, msgpack_encode } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import { CustomerMessage } from "recommend-library";
import * as bluebird from "bluebird";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import * as http from "http";
import * as queryString from "querystring";
import * as Disq from "hive-disque";


export const listener = new BusinessEventListener("wallet-events-disque");
const wxhost = process.env["WX_ENV"] === "test" ? "dev.fengchaohuzhu.com" : "m.fengchaohuzhu.com";
const log = bunyan.createLogger({
  name: "wallet-listener",
  streams: [
    {
      level: "info",
      path: "/var/log/wallet-listener-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/wallet-listener-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

listener.onEvent(async function (ctx: BusinessEventContext, data: Object) {
  const type = data["type"];
  const order_type = data["order_type"];

  let rep: Object;
  switch (type) {
    case 0:
      rep = await recharge(ctx, data);
      break;

    default:
      break;
  }
  return rep;
});

async function recharge(ctx: BusinessEventContext, data: Object): Promise<Object> {
  const db: PGClient = ctx.db;
  // const cache: RedisClient = ctx.cache;
  try {
    const qid: string = data["qid"];
    const result_of_plan_order = await rpc<Object>(ctx.domain, process.env["WALLET"], ctx.uid, "getPlanOrderByQid", qid);
    if (result_of_plan_order["code"] !== 200) {
      return { code: 400, msg: result_of_plan_order["msg"] };
    }

    let plan_order: Object = result_of_plan_order["data"];
    if (plan_order["state_code"] !== 2) {
      return { code: 400, msg: "该订单状态不为 2， 而是 " + plan_order["state_code"] };
    }

    // ` wallet 的 id 其实就是 user id
    let wallet: Object;
    let uid: string;
    let frozen: number;
    let cashable: number;
    let balance: number;
    let evtid: string;

    let wid: string = plan_order["vehicle"]["uid"];
    const result_of_wid: QueryResult = await db.query("SELECT * FROM wallet WHERE id = $1", [wid]);

    if (result_of_wid["rowCount"] === 0) {
      // TODO: Need to create a wallet
      const result_of_create_wallet: QueryResult = await db.query("INSERT INTO wallets(id, uid, frozen, cashable, balance, evtid) VALUES($1, $2, $3, $4, $5, $6)", [wid, uid, frozen, cashable, balance, evtid]);
    } else {
      const result_of_create_wallet: QueryResult = await db.query("UPDATE wallets SET frozen = $1, cashable = $2, balance = $3, evtid = $4 WHERE id = $5", [frozen, cashable, balance, evtid, wid]);
    }

    let vid: string = result_of_plan_order["vid"];
    const result_of_aid: QueryResult = await db.query("SELECT id FROM accounts WHERE id = $1", [vid]);
    let aid: string;
    let balance0: number;
    let balance1: number;
    let balance2: number;
    if (result_of_aid["rowCount"] === 0) {
      // TODO: create an account
      // id 	uuid 			primary 	
      // uid 	uuid 				users
      // vid 	uuid 	✓ 			vehicles
      // balance0 	float 				
      // balance1 	float 				
      // balance2 	float 				
      // created_at 	timestamp 		now 		
      // updated_at 	timestamp 		now 		
      // deleted 	boolean 		false
      let aid: string = uuid.v1();
      const result_of_create_account: QueryResult = await db.query("INSERT INTO account(id, uid, vid, balance0, balance1, balance2) VALUES($1, $2, $3, $4, $5, $6)", [aid, uid, vid, balance0, balance1, balance2]);
    } else {
      const result_of_create_account: QueryResult = await db.query("UPDATE account SET balance0 = $1, balance1 = $2, balance2 = $3 WHERE id = $4", [balance0, balance1, balance2]);
    }

    // id 	uuid 			primary 	
    // aid 	uuid 				accounts
    // type 	smallint 				
    // title 	char(128) 				
    // amount 	float 				
    // occurred_at 	timestamp 		now
    let tid: string = uuid.v1();
    let type: number = 1 // 1 for recharge
    let title: string = "充值";
    let amount: number = result_of_plan_order["summary"] // ? result_of_plan_order["payment"];
    const result_of_create_transaction: QueryResult = await db.query("INSERT INTO transactions(id, aid, type, title, amount) VALUES($1, $2, $3, $4, $5)", [tid, aid, type, title, amount]);

    let summary: number = Number(result_of_plan_order["summary"]);
    let payment: number = Number(result_of_plan_order["payment"]);
    if (summary > payment) {
      let tid: string = uuid.v1();
      let type: number; // ?
      let title: string; // = "充值";
      let amount: number = result_of_plan_order["payment"];// result_of_plan_order["summary"] 
      const result_of_create_transaction: QueryResult = await db.query("INSERT INTO transactions(id, aid, type, title, amount) VALUES($1, $2, $3, $4, $5)", [tid, aid, type, title, amount]);
    }

    await db.query("COMMIT");
    await db.release();
    return { code: 200, data: "Success" };

  } catch (e) {
    log.info(e);
    db.query("ROLLBACK");
    await db.release();
    return { code: 500, msg: e };
  }
}