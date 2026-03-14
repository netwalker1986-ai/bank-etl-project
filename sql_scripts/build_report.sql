-- ЭТАП: Формирование итоговой витрины мошеннических операций

DELETE FROM "BANK".rep_fraud
WHERE report_dt = CURRENT_DATE;

WITH base AS (
    SELECT
        t.trans_id,
        t.trans_date,
        t.card_num,
        t.oper_type,
        t.amt,
        t.oper_result,
        t.terminal,
        term.terminal_city,
        c.last_name || ' ' || c.first_name || ' ' || c.patronymic AS fio,
        c.phone,
        c.passport_num,
        c.passport_valid_to,
        a.valid_to AS account_valid_to
    FROM "BANK".dwh_fact_transactions t
    JOIN "BANK".cards cr
        ON t.card_num = cr.card_num
    JOIN "BANK".accounts a
        ON cr.account_num = a.account_num
    JOIN "BANK".clients c
        ON a.client = c.client_id
    JOIN "BANK".dwh_dim_terminals term
        ON t.terminal = term.terminal_id
),

rule_1 AS (
    SELECT
        b.trans_date AS event_dt,
        b.passport_num AS passport,
        b.fio,
        b.phone,
        'Операция при просроченном или заблокированном паспорте' AS event_type
    FROM base b
    LEFT JOIN "BANK".dwh_fact_passport_blacklist pbl
        ON b.passport_num = pbl.passport_num
    WHERE
        b.passport_valid_to < b.trans_date::date
        OR (
            pbl.passport_num IS NOT NULL
            AND pbl.entry_dt <= b.trans_date::date
        )
),

rule_2 AS (
    SELECT
        b.trans_date AS event_dt,
        b.passport_num AS passport,
        b.fio,
        b.phone,
        'Операция при недействующем договоре' AS event_type
    FROM base b
    WHERE b.account_valid_to < b.trans_date::date
),

rule_3 AS (
    SELECT
        x.trans_date AS event_dt,
        x.passport_num AS passport,
        x.fio,
        x.phone,
        'Операции в разных городах в течение одного часа' AS event_type
    FROM (
        SELECT
            b.*,
            LAG(b.trans_date) OVER (PARTITION BY b.passport_num ORDER BY b.trans_date) AS prev_trans_date,
            LAG(b.terminal_city) OVER (PARTITION BY b.passport_num ORDER BY b.trans_date) AS prev_city
        FROM base b
    ) x
    WHERE
        x.prev_trans_date IS NOT NULL
        AND x.trans_date - x.prev_trans_date <= INTERVAL '1 hour'
        AND x.terminal_city <> x.prev_city
),

rule_4 AS (
    SELECT
        x.trans_date AS event_dt,
        x.passport_num AS passport,
        x.fio,
        x.phone,
        'Попытка подбора суммы' AS event_type
    FROM (
        SELECT
            b.*,
            LAG(b.amt, 1) OVER (PARTITION BY b.passport_num ORDER BY b.trans_date) AS prev_amt_1,
            LAG(b.amt, 2) OVER (PARTITION BY b.passport_num ORDER BY b.trans_date) AS prev_amt_2,
            LAG(b.amt, 3) OVER (PARTITION BY b.passport_num ORDER BY b.trans_date) AS prev_amt_3,
            LAG(b.oper_result, 1) OVER (PARTITION BY b.passport_num ORDER BY b.trans_date) AS prev_result_1,
            LAG(b.oper_result, 2) OVER (PARTITION BY b.passport_num ORDER BY b.trans_date) AS prev_result_2,
            LAG(b.oper_result, 3) OVER (PARTITION BY b.passport_num ORDER BY b.trans_date) AS prev_result_3,
            LAG(b.trans_date, 3) OVER (PARTITION BY b.passport_num ORDER BY b.trans_date) AS trans_date_3_steps_back
        FROM base b
    ) x
    WHERE
        x.oper_result = 'SUCCESS'
        AND x.prev_result_1 = 'REJECT'
        AND x.prev_result_2 = 'REJECT'
        AND x.prev_result_3 = 'REJECT'
        AND x.amt < x.prev_amt_1
        AND x.prev_amt_1 < x.prev_amt_2
        AND x.prev_amt_2 < x.prev_amt_3
        AND x.trans_date - x.trans_date_3_steps_back <= INTERVAL '20 minutes'
)

INSERT INTO "BANK".rep_fraud (
    event_dt,
    passport,
    fio,
    phone,
    event_type,
    report_dt
)
SELECT event_dt, passport, fio, phone, event_type, CURRENT_DATE FROM rule_1
UNION ALL
SELECT event_dt, passport, fio, phone, event_type, CURRENT_DATE FROM rule_2
UNION ALL
SELECT event_dt, passport, fio, phone, event_type, CURRENT_DATE FROM rule_3
UNION ALL
SELECT event_dt, passport, fio, phone, event_type, CURRENT_DATE FROM rule_4;

SELECT count(*) FROM "BANK".rep_fraud;
