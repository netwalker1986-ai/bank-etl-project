-- STG для транзакций
CREATE TABLE IF NOT EXISTS STG_TRANSACTIONS (
    trans_id        VARCHAR(50),
    trans_date      VARCHAR(50),
    card_num        VARCHAR(50),
    oper_type       VARCHAR(50),
    amt             VARCHAR(50),
    oper_result     VARCHAR(50),
    terminal        VARCHAR(50)
);

-- STG для терминалов
CREATE TABLE IF NOT EXISTS STG_TERMINALS (
    terminal_id     VARCHAR(50),
    terminal_type   VARCHAR(50),
    terminal_city   VARCHAR(100),
    terminal_address VARCHAR(200)
);

-- STG для черного списка паспортов
CREATE TABLE IF NOT EXISTS STG_PASSPORT_BLACKLIST (
    passport_num    VARCHAR(50),
    entry_dt        VARCHAR(50)
);