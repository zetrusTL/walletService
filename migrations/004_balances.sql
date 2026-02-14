CREATE TABLE IF NOT EXISTS balances (
  wallet_id BIGINT NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
  currency TEXT NOT NULL,
  amount NUMERIC(18,2) NOT NULL DEFAULT 0,
  PRIMARY KEY (wallet_id, currency),
  CONSTRAINT chk_currency CHECK (currency IN ('USD', 'RUB', 'EUR'))
);
