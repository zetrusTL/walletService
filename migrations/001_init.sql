-- Legacy table for UUID-based wallets (optional backward compatibility)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS wallet_legacy (
  id UUID PRIMARY KEY,
  balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0)
);

INSERT INTO wallet_legacy (id, balance) VALUES
  ('11111111-1111-1111-1111-111111111111', 0),
  ('22222222-2222-2222-2222-222222222222', 10000)
ON CONFLICT (id) DO NOTHING;
