export interface AccountEvent {
  id: string;
  type: number;
  opid: string;
  uid?: string;
  aid?: string;
  amount: number;
  occurred_at: Date;
  oid?: string;
  vid?: string;
  maid?: string;
};

export interface Account {
  id: string;
  vid: string;
  uid: string;
  balance0: number;
  balance1: number;
  bonus: number;
  frozen_balance0: number;
  frozen_balance1: number;
  cashable_balance: number;
  evtid: string;
  created_at: Date;
  updated_at: Date;
  vehicle?: any;
};

export interface Wallet {
  uid?: string;
  frozen: number;
  cashable: number;
  balance: number;
  accounts: Account[];
};

export interface Transaction {
  id: string;
  type: number;
  aid: string;
  uid: string;
  title: string;
  license?: string;
  amount: number;
  occurred_at: Date;
  oid?: string;
  maid?: string;
  sn?: string;
};

export interface TransactionEvent {
  id: string;
  type: number;
  uid?: string;
  title: string;
  license?: string;
  amount: number;
  occurred_at: Date;
  oid?: string;
  vid?: string;
  aid?: string;
  maid?: string;
  sn?: string;
};
