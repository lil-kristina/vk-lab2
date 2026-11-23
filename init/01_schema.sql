-- Таблица-справочник счетов
CREATE TABLE IF NOT EXISTS accounts (
    account_id   BIGINT PRIMARY KEY,
    owner        VARCHAR(120) NOT NULL,
    city         VARCHAR(80)  NOT NULL,
    status       VARCHAR(20)  NOT NULL,   -- active / frozen / closed
    balance      NUMERIC(14,2) NOT NULL,
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

-- Начальные данные
INSERT INTO accounts (account_id, owner, city, status, balance, updated_at) VALUES
  (1, 'Иванов Иван',    'Москва',    'active', 1000.00, '2025-01-10 09:00:00'),
  (2, 'Петрова Анна',   'СПб',       'active', 2500.00, '2025-01-11 10:30:00'),
  (3, 'Сидоров Пётр',   'Казань',    'frozen',  700.00, '2025-01-12 12:15:00'),
  (4, 'Кузнецова Ольга','Новосибирск','active', 4300.00, '2025-01-13 14:00:00')
ON CONFLICT (account_id) DO NOTHING;

-- Важно для Debezium: чтобы в UPDATE/DELETE были видны старые значения
ALTER TABLE accounts REPLICA IDENTITY FULL;

-- ТРИГГЕР ДЛЯ АВТОМАТИЧЕСКОГО ОБНОВЛЕНИЯ updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();