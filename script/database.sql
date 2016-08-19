CREATE TABLE plans (
  id uuid PRIMARY KEY,
  title char(128) NOT NULL,
  description text NULL,
  image char(1024) NOT NULL,
  thumbnail char(1024) NOT NULL,
  period integer NOT NULL DEFAULT 0
);

CREATE TABLE plan_rules (
  id uuid PRIMARY KEY,
  pid uuid NOT NULL REFERENCES plans ON DELETE CASCADE,
  name char(128) NULL,
  title char(128) NULL,
  description text NULL
);

CREATE TABLE plan_items (
  id uuid PRIMARY KEY,
  pid uuid NOT NULL REFERENCES plans ON DELETE CASCADE,
  title char(128) NULL,
  description text NULL,
  price float NOT NULL DEFAULT 0.0
);

