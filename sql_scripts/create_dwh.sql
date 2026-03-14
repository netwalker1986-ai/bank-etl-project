-- =========================
-- ФАКТЫ
-- =========================

CREATE TABLE IF NOT EXISTS DWH_FACT_TRANSACTIONS (
    trans_id        VARCHAR(50) PRIMARY KEY,
    trans_date      TIMESTAMP,
    card_num        VARCHAR(50),
    oper_type       VARCHAR(50),
    amt             NUMERIC(18,2),
    oper_result     VARCHAR(50),
    terminal        VARCHAR(50),
    create_dt       TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS DWH_FACT_PASSPORT_BLACKLIST (
    passport_num    VARCHAR(50),
    entry_dt        DATE,
    create_dt       TIMESTAMP DEFAULT now()
);


-- =========================
-- ИЗМЕРЕНИЕ TERMINALS (SCD1)
-- =========================

CREATE TABLE IF NOT EXISTS DWH_DIM_TERMINALS (
    terminal_id      VARCHAR(50) PRIMARY KEY,
    terminal_type    VARCHAR(50),
    terminal_city    VARCHAR(100),
    terminal_address VARCHAR(200),
    create_dt        TIMESTAMP DEFAULT now(),
    update_dt        TIMESTAMP
);


-- =========================
-- ВИТРИНА
-- =========================

CREATE TABLE IF NOT EXISTS REP_FRAUD (
    event_dt    TIMESTAMP,
    passport    VARCHAR(50),
    fio         VARCHAR(200),
    phone       VARCHAR(50),
    event_type  VARCHAR(200),
    report_dt   TIMESTAMP
);