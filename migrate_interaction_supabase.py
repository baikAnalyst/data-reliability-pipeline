"""로컬 interaction 스키마를 Supabase m3_interaction 스키마로 추가합니다.

기존 m3_raw, m3_processed, m3_derived, m3_monitoring 스키마는 수정하지 않습니다.
"""

import os
from pathlib import Path
from urllib.parse import urlsplit

import psycopg
from psycopg import sql
from dotenv import load_dotenv


ENV_PATH = Path(__file__).resolve().parent / "collector" / ".env"

SOURCE_SCHEMA = "interaction"
TARGET_SCHEMA = "m3_interaction"

EXPECTED_TABLES = {
    "recommendation_interactions",
    "chatbot_interactions",
    "search_events",
    "facility_click_events",
    "user_feedback",
}


def connect_databases():
    load_dotenv(ENV_PATH, override=False)

    if os.getenv("DB_HOST") not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("Source DB_HOST must be local")

    remote_url = os.environ["SUPABASE_DATABASE_URL"]

    if not (urlsplit(remote_url).hostname or "").endswith(".supabase.com"):
        raise ValueError(
            "Target must be the configured Supabase PostgreSQL host"
        )

    source = psycopg.connect(
        host=os.environ["DB_HOST"],
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        autocommit=True,
    )

    target = psycopg.connect(
        remote_url,
        autocommit=False,
        connect_timeout=15,
    )

    return source, target


def check_source_tables(source):
    with source.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (SOURCE_SCHEMA,),
        )

        actual = {row[0] for row in cursor}

    if actual != EXPECTED_TABLES:
        raise ValueError(
            "Source interaction table list differs from expected plan.\n"
            f"expected={sorted(EXPECTED_TABLES)}\n"
            f"actual={sorted(actual)}"
        )

    return sorted(actual)


def check_target(target):
    with target.cursor() as cursor:
        cursor.execute(
            """
            SELECT 1
            FROM pg_namespace
            WHERE nspname = %s
            """,
            (TARGET_SCHEMA,),
        )

        if cursor.fetchone():
            raise ValueError(
                f"{TARGET_SCHEMA} already exists. "
                "Migration stopped to protect existing data."
            )

        cursor.execute(
            """
            SELECT has_database_privilege(
                current_user,
                current_database(),
                'CREATE'
            )
            """
        )

        if not cursor.fetchone()[0]:
            raise ValueError(
                "Target role cannot create schema."
            )

    target.rollback()


def get_columns(source, table):
    with source.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                a.attname,
                pg_catalog.format_type(a.atttypid, a.atttypmod),
                a.attnotnull,
                pg_get_expr(d.adbin, d.adrelid)
            FROM pg_attribute a
            JOIN pg_class c
              ON c.oid = a.attrelid
            JOIN pg_namespace n
              ON n.oid = c.relnamespace
            LEFT JOIN pg_attrdef d
              ON d.adrelid = c.oid
             AND d.adnum = a.attnum
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (SOURCE_SCHEMA, table),
        )

        return cursor.fetchall()


def create_table(source, target, table):
    columns = get_columns(source, table)

    definitions = []

    for name, data_type, required, default in columns:
        # BIGSERIAL 컬럼의 로컬 sequence 이름은 Supabase에 존재하지 않으므로
        # BIGSERIAL로 새 sequence를 생성합니다.
        if (
            data_type == "bigint"
            and default
            and default.startswith("nextval(")
        ):
            definition = sql.SQL("{} BIGSERIAL").format(
                sql.Identifier(name)
            )
        else:
            parts = [
                sql.Identifier(name),
                sql.SQL(data_type),
            ]

            if required:
                parts.append(sql.SQL("NOT NULL"))

            if default:
                # 현재 interaction 테이블에서 사용하는 시간 기본값만 허용
                if default.lower() in {
                    "current_timestamp",
                    "now()",
                }:
                    parts.append(
                        sql.SQL("DEFAULT CURRENT_TIMESTAMP")
                    )

            definition = sql.SQL(" ").join(parts)

        definitions.append(definition)

    with target.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                "CREATE TABLE {}.{} ({})"
            ).format(
                sql.Identifier(TARGET_SCHEMA),
                sql.Identifier(table),
                sql.SQL(", ").join(definitions),
            )
        )


def copy_primary_key(source, target, table):
    with source.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                con.conname,
                pg_get_constraintdef(con.oid)
            FROM pg_constraint con
            JOIN pg_class c
              ON c.oid = con.conrelid
            JOIN pg_namespace n
              ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND con.contype = 'p'
            """,
            (SOURCE_SCHEMA, table),
        )

        constraints = cursor.fetchall()

    with target.cursor() as cursor:
        for constraint_name, definition in constraints:
            cursor.execute(
                sql.SQL(
                    "ALTER TABLE {}.{} "
                    "ADD CONSTRAINT {} {}"
                ).format(
                    sql.Identifier(TARGET_SCHEMA),
                    sql.Identifier(table),
                    sql.Identifier(constraint_name),
                    sql.SQL(definition),
                )
            )


def verify_table(source, target, table):
    source_name = sql.SQL("{}.{}").format(
        sql.Identifier(SOURCE_SCHEMA),
        sql.Identifier(table),
    )

    target_name = sql.SQL("{}.{}").format(
        sql.Identifier(TARGET_SCHEMA),
        sql.Identifier(table),
    )

    with source.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT count(*) FROM {}").format(
                source_name
            )
        )
        source_count = cursor.fetchone()[0]

    with target.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT count(*) FROM {}").format(
                target_name
            )
        )
        target_count = cursor.fetchone()[0]

    if source_count != 0:
        raise ValueError(
            f"{SOURCE_SCHEMA}.{table} is not empty: "
            f"rows={source_count}"
        )

    if target_count != 0:
        raise ValueError(
            f"{TARGET_SCHEMA}.{table} is not empty: "
            f"rows={target_count}"
        )

    return target_count


def main():
    source, target = connect_databases()

    try:
        tables = check_source_tables(source)
        check_target(target)

        print(
            f"source_schema={SOURCE_SCHEMA} "
            f"target_schema={TARGET_SCHEMA} "
            f"tables={len(tables)}"
        )

        print("tables:")
        for table in tables:
            print(f"  - {table}")

        # 여기까지 모두 검증된 뒤에만 Supabase 쓰기 시작
        with target.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE SCHEMA {}").format(
                    sql.Identifier(TARGET_SCHEMA)
                )
            )

        target.commit()

        print(
            f"[OK] schema created: {TARGET_SCHEMA}",
            flush=True,
        )

        for table in tables:
            try:
                create_table(source, target, table)
                copy_primary_key(source, target, table)

                count = verify_table(
                    source,
                    target,
                    table,
                )

                target.commit()

                print(
                    f"[OK] {SOURCE_SCHEMA}.{table} "
                    f"-> {TARGET_SCHEMA}.{table} "
                    f"rows={count}",
                    flush=True,
                )

            except Exception:
                target.rollback()
                raise

        print(
            f"\n[DONE] migration_complete "
            f"schema={TARGET_SCHEMA} "
            f"tables={len(tables)} "
            f"rows=0",
            flush=True,
        )

    finally:
        source.close()
        target.close()


if __name__ == "__main__":
    main()