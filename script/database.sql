
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
-- 互助模块
CREATE TABLE mutual_aids(
  id uuid PRIMARY KEY,
  no char(40) NOT NULL,
  pid uuid NOT NULL REFERENCES plans,
  uid uuid NOT NULL REFERENCES users ON DELETE CASCADE,
  driver_id uuid NOT NULL, 
  vin char(30) NOT NULL,
  city char(40) NOT NULL,
  district char(40) NOT NULL,
  street char(100) NOT NULL,
  phone char(20) NOT NULL,
  occurred_at timestamp NOT NULL,
  responsibility char(20) NOT NULL,
  situation text NOT NULL,
  description text NOT NULL,
  scene_view char(1024) NOT NULL,
  vehicle_damaged_view char(1024) NOT NULL,
  vehicle_frontal_view char(1024) NOT NULL,
  driver_view char(1024) NOT NULL,
  driver_license_view char(1024) NOT NULL,
  state boolean NOT NULL
);
CREATE TABLE recompense(
  id uuid PRIMARY KEY,
  mid uuid NOT NULL REFERENCES mutual_aids ON DELETE CASCADE,
  personal_fee char(20) NOT NULL,
  personal_balance numeric NOT NULL,
  small_hive_fee char(20) NOT NULL,
  small_hive_balance numeric NOT NULL,
  big_hive_fee char(20) NOT NULL,
  big_hive_balance numeric NOT NULL,
  paid_at timestamp NOT NULL
);
-- 订单模块
CREATE TABLE orders(
  id uuid PRIMARY KEY,
  no char(20) NOT NULL,
  service_ratio char(4) NOT NULL,
  price numeric NOT NULL,
  actual_price numeric NOT NULL,
  hive_ratio char(5) NOT NULL,
  small_hive_balance numeric NOT NULL,
  big_hive_balance numeric NOT NULL,
  duration_begin timestamp NOT NULL,
  duration_end timestamp NOT NULL,
  discount char(5) NOT NULL,
  discount_ratio char(5) NOT NULL,
  hive_id uuid NOT NULL
);
CREATE TABLE driver(
  did uuid PRIMARY KEY,
  order_id uuid NOT NULL REFERENCES orders ON DELETE CASCADE,
  mutual_aid_id uuid NOT NULL REFERENCES mutual_aids ON DELETE CASCADE,
  name char(40) NOT NULL,
  gender char(4) NOT NULL,
  identity_no char(18) NOT NULL,
  phone char(20) NOT NULL,
  identity_frontal_view char(1024) NOT NULL,
  identity_rear_view char(1024) NOT NULL
);
