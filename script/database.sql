
-- 钱包模块
CREATE TABLE wallets(id uuid PRIMARY KEY,balance numeric NOT NULL);
CREATE TABLE accounts(
  id uuid PRIMARY KEY,
  wid uuid NOT NULL REFERENCES wallets ON DELETE CASCADE,
  type boolean NOT NULL,
  vid uuid REFERENCES vehicles NOT NULL,
  balance0 numeric NOT NULL,
  balance1 numeric NOT NULL
);
CREATE TABLE transations(
  id uuid PRIMARY KEY,
  aid uuid NOT NULL REFERENCES accounts ON DELETE CASCADE,
  type integer NOT NULL,
  title char(100) NOT NULL,
  occurred_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  amount integer NOT NULL   
);
