CREATE TABLE IF NOT EXISTS currency_rates (
  from_currency TEXT NOT NULL,
  to_currency   TEXT NOT NULL,
  rate          DOUBLE PRECISION NOT NULL,
  updated_at    TIMESTAMP NOT NULL DEFAULT now(),
  PRIMARY KEY (from_currency, to_currency)
);

-- Seed base pairs USD/RUB/EUR (6 directions)
INSERT INTO currency_rates (from_currency, to_currency, rate) VALUES
  ('USD', 'RUB', 100.50),
  ('RUB', 'USD', 0.00995),
  ('USD', 'EUR', 0.92),
  ('EUR', 'USD', 1.087),
  ('EUR', 'RUB', 109.20),
  ('RUB', 'EUR', 0.00916)
ON CONFLICT (from_currency, to_currency) DO UPDATE SET rate = EXCLUDED.rate, updated_at = now();
