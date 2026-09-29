"""
mysql_manager.py
-------------------
The GOLD warehouse lives in a MySQL database (a separate schema, e.g.
"classicmodels_dw", in the SAME Docker MySQL container the user already
runs for the source "classicmodels" database) instead of a local DuckDB
file. Browsable directly in DBeaver like the rest of the project.

Same public interface as DuckDBManager (ensure_schema, table_exists,
read_table, get_max_surrogate_key, overwrite_table, append_table) so
curated_to_gold.py needs only a one-line import swap to switch targets.

Design notes / MySQL-specific differences from the DuckDB version:
  - MySQL has no "INSERT ... BY NAME". Instead we rely on pandas'
    to_sql(), which always generates an INSERT with EXPLICIT column
    names taken from the DataFrame - so a DataFrame whose column order
    doesn't match the table's declared column order still lands
    correctly. This is the MySQL equivalent protection for the exact
    bug we hit with DuckDB's positional INSERT.
  - overwrite_table() does TRUNCATE + to_sql(append) inside one
    connection so it stays a single atomic-ish swap from the caller's
    point of view (not multi-statement-transaction safe across a crash
    mid-way, which is an accepted local-project simplification).
  - BOOLEAN is stored as TINYINT(1) (MySQL's usual mapping) and read
    back as an int 0/1; callers that need real bool should cast.
"""

import os

import pandas as pd
from sqlalchemy import create_engine, text


DDL_STATEMENTS = {
    "dim_date": """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key INT PRIMARY KEY,
            full_date DATE,
            day_name VARCHAR(20),
            day_of_week INT,
            day_of_month INT,
            week_of_year INT,
            month_num INT,
            month_name VARCHAR(20),
            quarter INT,
            year INT,
            is_weekend TINYINT(1)
        )
    """,
    "dim_office": """
        CREATE TABLE IF NOT EXISTS dim_office (
            office_key INT PRIMARY KEY,
            office_code VARCHAR(20),
            city VARCHAR(100),
            phone VARCHAR(50),
            address_line1 VARCHAR(200),
            address_line2 VARCHAR(200),
            state VARCHAR(100),
            country VARCHAR(100),
            postal_code VARCHAR(20),
            territory VARCHAR(50)
        )
    """,
    "dim_product_line": """
        CREATE TABLE IF NOT EXISTS dim_product_line (
            product_line_key INT PRIMARY KEY,
            product_line VARCHAR(100),
            text_description TEXT
        )
    """,
    "dim_order_status": """
        CREATE TABLE IF NOT EXISTS dim_order_status (
            order_status_key INT PRIMARY KEY,
            status_code VARCHAR(50)
        )
    """,
    "dim_employee": """
        CREATE TABLE IF NOT EXISTS dim_employee (
            employee_key INT PRIMARY KEY,
            employee_number INT,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            extension VARCHAR(20),
            email VARCHAR(150),
            office_key INT,
            job_title VARCHAR(100),
            row_hash VARCHAR(64),
            row_effective_date DATE,
            row_end_date DATE,
            is_current TINYINT(1)
        )
    """,
    "dim_product": """
        CREATE TABLE IF NOT EXISTS dim_product (
            product_key INT PRIMARY KEY,
            product_code VARCHAR(20),
            product_name VARCHAR(150),
            product_line_key INT,
            product_scale VARCHAR(20),
            product_vendor VARCHAR(100),
            product_description TEXT,
            msrp DECIMAL(10,2),
            buy_price DECIMAL(10,2),
            row_hash VARCHAR(64),
            row_effective_date DATE,
            row_end_date DATE,
            is_current TINYINT(1)
        )
    """,
    "dim_customer": """
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_key INT PRIMARY KEY,
            customer_number INT,
            customer_name VARCHAR(150),
            contact_last_name VARCHAR(100),
            contact_first_name VARCHAR(100),
            phone VARCHAR(50),
            address_line1 VARCHAR(200),
            address_line2 VARCHAR(200),
            city VARCHAR(100),
            state VARCHAR(100),
            postal_code VARCHAR(20),
            country VARCHAR(100),
            sales_rep_employee_key INT,
            credit_limit DECIMAL(10,2),
            row_hash VARCHAR(64),
            row_effective_date DATE,
            row_end_date DATE,
            is_current TINYINT(1)
        )
    """,
    "fact_order_line": """
        CREATE TABLE IF NOT EXISTS fact_order_line (
            order_date_key INT,
            required_date_key INT,
            shipped_date_key INT,
            customer_key INT,
            employee_key INT,
            product_key INT,
            order_status_key INT,
            order_number INT,
            order_line_number INT,
            quantity_ordered INT,
            price_each DECIMAL(10,2),
            buy_price_at_order DECIMAL(10,2),
            msrp_at_order DECIMAL(10,2),
            extended_sales_amount DECIMAL(12,2),
            extended_cost_amount DECIMAL(12,2),
            extended_margin_amount DECIMAL(12,2)
        )
    """,
    "fact_payment": """
        CREATE TABLE IF NOT EXISTS fact_payment (
            payment_date_key INT,
            customer_key INT,
            check_number VARCHAR(50),
            payment_amount DECIMAL(12,2)
        )
    """,
}

TABLE_CREATE_ORDER = [
    "dim_date", "dim_office", "dim_product_line", "dim_order_status",
    "dim_employee", "dim_product", "dim_customer",
    "fact_order_line", "fact_payment",
]


class MySQLManager:
    """
    Same public interface as DuckDBManager, backed by MySQL instead.

    mysql_cfg example (from secrets.yaml -> warehouse.mysql):
        host: "localhost"
        port: 3306
        database: "classicmodels_dw"
        user: "root"
        password: "..."
    """

    def __init__(self, mysql_cfg: dict):
        self.mysql_cfg = mysql_cfg
        self.database = mysql_cfg["database"]
        self._ensure_database_exists()
        self.engine = create_engine(self._connection_url())

    def _server_url(self) -> str:
        c = self.mysql_cfg
        return f"mysql+pymysql://{c['user']}:{c['password']}@{c['host']}:{c['port']}"

    def _connection_url(self) -> str:
        return f"{self._server_url()}/{self.database}"

    def _ensure_database_exists(self):
        """Creates the gold database itself (e.g. classicmodels_dw) if it
        doesn't exist yet, using a server-level connection (no database
        selected). Safe to call every run."""
        server_engine = create_engine(self._server_url())
        try:
            with server_engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{self.database}`"))
                conn.commit()
        finally:
            server_engine.dispose()

    def ensure_schema(self):
        with self.engine.connect() as conn:
            for table in TABLE_CREATE_ORDER:
                conn.execute(text(DDL_STATEMENTS[table]))
            conn.commit()

    def table_exists(self, table_name: str) -> bool:
        with self.engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = :db AND table_name = :tbl"
                ),
                {"db": self.database, "tbl": table_name},
            ).fetchone()
            return result[0] > 0

    def read_table(self, table_name: str) -> pd.DataFrame:
        if not self.table_exists(table_name):
            return pd.DataFrame()
        with self.engine.connect() as conn:
            return pd.read_sql(f"SELECT * FROM `{table_name}`", conn)

    def get_max_surrogate_key(self, table_name: str, key_column: str) -> int:
        if not self.table_exists(table_name):
            return 0
        with self.engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COALESCE(MAX(`{key_column}`), 0) FROM `{table_name}`")
            ).fetchone()
            return int(result[0])

    def overwrite_table(self, table_name: str, df: pd.DataFrame):
        """Replaces the ENTIRE contents of a table with df.

        pandas.to_sql() writes an INSERT with explicit column names taken
        from the DataFrame, so a mismatched column order (vs. the DDL)
        still lands in the right columns - the MySQL equivalent of
        DuckDB's "INSERT ... BY NAME" protection.
        """
        with self.engine.begin() as conn:
            conn.execute(text(DDL_STATEMENTS[table_name]))
            conn.execute(text(f"TRUNCATE TABLE `{table_name}`"))
            if not df.empty:
                df.to_sql(table_name, conn, if_exists="append", index=False)

    def append_table(self, table_name: str, df: pd.DataFrame):
        if df.empty:
            return
        with self.engine.begin() as conn:
            conn.execute(text(DDL_STATEMENTS[table_name]))
            df.to_sql(table_name, conn, if_exists="append", index=False)
