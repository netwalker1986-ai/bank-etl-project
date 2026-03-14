import os
import glob
import shutil
import psycopg
import pandas as pd
from dotenv import load_dotenv


ARCHIVE_DIR = "archive"


def get_connection():
    load_dotenv()

    return psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def ensure_archive_dir():
    os.makedirs(ARCHIVE_DIR, exist_ok=True)


def archive_file(filepath):
    ensure_archive_dir()

    filename = os.path.basename(filepath)
    archived_name = f"{filename}.backup"
    archived_path = os.path.join(ARCHIVE_DIR, archived_name)

    if os.path.exists(archived_path):
        os.remove(archived_path)

    shutil.move(filepath, archived_path)
    print(f"Файл перемещен в архив: {archived_path}")


def load_one_transaction_file(connection, filepath):
    with connection.cursor() as cursor:
        cursor.execute('TRUNCATE TABLE "BANK".stg_transactions')

        with open(filepath, "r", encoding="utf-8") as f:
            with cursor.copy(
                '''
                COPY "BANK".stg_transactions (
                    trans_id,
                    trans_date,
                    amt,
                    card_num,
                    oper_type,
                    oper_result,
                    terminal
                )
                FROM STDIN
                WITH (
                    FORMAT csv,
                    DELIMITER ';',
                    HEADER true
                )
                '''
            ) as copy:
                copy.write(f.read())

    connection.commit()
    print(f"STG transactions загружена из файла: {filepath}")


def load_one_passport_file(connection, filepath):
    df = pd.read_excel(filepath, dtype=str)

    df = df.rename(
        columns={
            "date": "entry_dt",
            "passport": "passport_num"
        }
    )

    df = df[["passport_num", "entry_dt"]]
    df = df.fillna("")

    records = list(df.itertuples(index=False, name=None))

    with connection.cursor() as cursor:
        cursor.execute('TRUNCATE TABLE "BANK".stg_passport_blacklist')

        for row in records:
            cursor.execute(
                '''
                INSERT INTO "BANK".stg_passport_blacklist (
                    passport_num,
                    entry_dt
                )
                VALUES (%s, %s)
                ''',
                row
            )

    connection.commit()
    print(f"STG passport_blacklist загружена из файла: {filepath}")


def load_one_terminal_file(connection, filepath):
    df = pd.read_excel(filepath, dtype=str)

    df = df[[
        "terminal_id",
        "terminal_type",
        "terminal_city",
        "terminal_address"
    ]]
    df = df.fillna("")

    records = list(df.itertuples(index=False, name=None))

    with connection.cursor() as cursor:
        cursor.execute('TRUNCATE TABLE "BANK".stg_terminals')

        for row in records:
            cursor.execute(
                '''
                INSERT INTO "BANK".stg_terminals (
                    terminal_id,
                    terminal_type,
                    terminal_city,
                    terminal_address
                )
                VALUES (%s, %s, %s, %s)
                ''',
                row
            )

    connection.commit()
    print(f"STG terminals загружена из файла: {filepath}")


def execute_sql_file(connection, filepath):
    with open(filepath, "r", encoding="utf-8") as file:
        sql_script = file.read()

    statements = sql_script.split(";")

    with connection.cursor() as cursor:
        for statement in statements:
            stmt = statement.strip()
            if stmt:
                cursor.execute(stmt)

    connection.commit()
    print(f"{filepath} выполнен")


def print_counts(connection):
    with connection.cursor() as cursor:
        cursor.execute('SELECT count(*) FROM "BANK".stg_transactions')
        stg_transactions_count = cursor.fetchone()[0]

        cursor.execute('SELECT count(*) FROM "BANK".dwh_fact_transactions')
        dwh_transactions_count = cursor.fetchone()[0]

        cursor.execute('SELECT count(*) FROM "BANK".stg_passport_blacklist')
        stg_passport_count = cursor.fetchone()[0]

        cursor.execute('SELECT count(*) FROM "BANK".dwh_fact_passport_blacklist')
        dwh_passport_count = cursor.fetchone()[0]

        cursor.execute('SELECT count(*) FROM "BANK".stg_terminals')
        stg_terminals_count = cursor.fetchone()[0]

        cursor.execute('SELECT count(*) FROM "BANK".dwh_dim_terminals')
        dwh_terminals_count = cursor.fetchone()[0]

        cursor.execute('SELECT count(*) FROM "BANK".rep_fraud')
        rep_fraud_count = cursor.fetchone()[0]

    print(f"STG transactions rows: {stg_transactions_count}")
    print(f"DWH transactions rows: {dwh_transactions_count}")
    print(f"STG passport rows: {stg_passport_count}")
    print(f"DWH passport rows: {dwh_passport_count}")
    print(f"STG terminals rows: {stg_terminals_count}")
    print(f"DWH terminals rows: {dwh_terminals_count}")
    print(f"REP fraud rows: {rep_fraud_count}")


def process_transactions(connection):
    files = sorted(glob.glob("transactions_*.txt"))

    if not files:
        print("Файлы transactions не найдены")
        return

    for filepath in files:
        print("===================================")
        print(f"Обрабатываем файл transactions: {filepath}")

        load_one_transaction_file(connection, filepath)
        print_counts(connection)

        execute_sql_file(connection, "sql_scripts/prepare_data.sql")
        print_counts(connection)

        archive_file(filepath)


def process_passports(connection):
    files = sorted(glob.glob("passport_blacklist_*.xlsx"))

    if not files:
        print("Файлы passport_blacklist не найдены")
        return

    for filepath in files:
        print("===================================")
        print(f"Обрабатываем файл passport_blacklist: {filepath}")

        load_one_passport_file(connection, filepath)
        print_counts(connection)

        execute_sql_file(connection, "sql_scripts/prepare_data.sql")
        print_counts(connection)

        archive_file(filepath)


def process_terminals(connection):
    files = sorted(glob.glob("terminals_*.xlsx"))

    if not files:
        print("Файлы terminals не найдены")
        return

    for filepath in files:
        print("===================================")
        print(f"Обрабатываем файл terminals: {filepath}")

        load_one_terminal_file(connection, filepath)
        print_counts(connection)

        execute_sql_file(connection, "sql_scripts/prepare_data.sql")
        print_counts(connection)

        archive_file(filepath)


def build_report(connection):
    print("========== BUILD REPORT ==========")
    execute_sql_file(connection, "sql_scripts/build_report.sql")
    print_counts(connection)


def main():
    conn = get_connection()

    try:
        print("========== TRANSACTIONS ==========")
        process_transactions(conn)

        print("========== PASSPORT BLACKLIST ==========")
        process_passports(conn)

        print("========== TERMINALS ==========")
        process_terminals(conn)

        build_report(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
