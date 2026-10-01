-- Source schema, a least-privilege replication role for Debezium, and the publication it reads.
-- Run by sql/init.sh with psql, which passes the Debezium password as :'debezium_password'.

CREATE TABLE customers (
  id         serial PRIMARY KEY,
  email      text NOT NULL UNIQUE,
  name       text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders (
  id          serial PRIMARY KEY,
  customer_id integer NOT NULL REFERENCES customers (id),
  amount      numeric(10, 2) NOT NULL CHECK (amount >= 0),
  status      text NOT NULL DEFAULT 'new',
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Full before-images, so update and delete events carry the old row.
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;

-- Debezium only needs to replicate and read; it does not own tables or create publications.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD :'debezium_password';
GRANT CONNECT ON DATABASE shop TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON customers, orders TO debezium;

CREATE PUBLICATION dbz_publication FOR TABLE customers, orders;

INSERT INTO customers (email, name) VALUES
  ('ada@example.com', 'Ada'),
  ('linus@example.com', 'Linus');
INSERT INTO orders (customer_id, amount) VALUES (1, 19.99), (2, 5.00);
