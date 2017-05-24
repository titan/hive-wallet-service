import { Person } from "person-library";

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
  undo: boolean;
  project: number;
  license?: string; // useful for project = 2 or project = 3
  owner?: Person; // only used for project = 2 or project = 3
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
  undo: boolean;
  project: number;
};
