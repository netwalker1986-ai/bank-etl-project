-- =========================
-- TRANSACTIONS → DWH_FACT
-- =========================

INSERT INTO "BANK".dwh_fact_transactions (
    trans_id,
    trans_date,
    card_num,
    oper_type,
    amt,
    oper_result,
    terminal,
    create_dt
)
SELECT
    s.trans_id,
    to_timestamp(s.trans_date, 'YYYY-MM-DD HH24:MI:SS'),
    s.card_num,
    s.oper_type,
    replace(s.amt, ',', '.')::numeric,
    s.oper_result,
    s.terminal,
    now()
FROM "BANK".stg_transactions s
WHERE NOT EXISTS (
    SELECT 1
    FROM "BANK".dwh_fact_transactions d
    WHERE d.trans_id = s.trans_id
);

-- =========================
-- PASSPORT BLACKLIST → FACT
-- =========================

INSERT INTO "BANK".dwh_fact_passport_blacklist (
    passport_num,
    entry_dt
)
SELECT
    s.passport_num,
    to_date(s.entry_dt, 'YYYY-MM-DD')
FROM "BANK".stg_passport_blacklist s
WHERE NOT EXISTS (
    SELECT 1
    FROM "BANK".dwh_fact_passport_blacklist d
    WHERE d.passport_num = s.passport_num
);


-- =========================
-- TERMINALS (SCD1)
-- =========================

UPDATE "BANK".dwh_dim_terminals d
SET
    terminal_type = s.terminal_type,
    terminal_city = s.terminal_city,
    terminal_address = s.terminal_address,
    update_dt = now()
FROM "BANK".stg_terminals s
WHERE d.terminal_id = s.terminal_id;


INSERT INTO "BANK".dwh_dim_terminals (
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address
)
SELECT
    s.terminal_id,
    s.terminal_type,
    s.terminal_city,
    s.terminal_address
FROM "BANK".stg_terminals s
WHERE NOT EXISTS (
    SELECT 1
    FROM "BANK".dwh_dim_terminals d
    WHERE d.terminal_id = s.terminal_id
);