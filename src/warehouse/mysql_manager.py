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

# Foreign keys, added as a separate ALTER-TABLE pass (after every table
# already exists) so DBeaver/any ERD tool can auto-draw the star schema's
# join lines. Named constraints so re-running ensure_schema() is idempotent
# (we check information_schema before adding, see _add_foreign_keys).
FOREIGN_KEYS = [
    # (constraint_name, child_table, child_column, parent_table, parent_column)
    ("fk_employee_office", "dim_employee", "office_key", "dim_office", "office_key"),
    ("fk_product_productline", "dim_product", "product_line_key", "dim_product_line", "product_line_key"),
    ("fk_customer_salesrep", "dim_customer", "sales_rep_employee_key", "dim_employee", "employee_key"),

    ("fk_fol_orderdate", "fact_order_line", "order_date_key", "dim_date", "date_key"),
    ("fk_fol_requireddate", "fact_order_line", "required_date_key", "dim_date", "date_key"),
    ("fk_fol_shippeddate", "fact_order_line", "shipped_date_key", "dim_date", "date_key"),
    ("fk_fol_customer", "fact_order_line", "customer_key", "dim_customer", "customer_key"),
    ("fk_fol_employee", "fact_order_line", "employee_key", "dim_employee", "employee_key"),
    ("fk_fol_product", "fact_order_line", "product_key", "dim_product", "product_key"),
    ("fk_fol_orderstatus", "fact_order_line", "order_status_key", "dim_order_status", "order_status_key"),

    ("fk_fp_paymentdate", "fact_payment", "payment_date_key", "dim_date", "date_key"),
    ("fk_fp_customer", "fact_payment", "customer_key", "dim_customer", "customer_key"),
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
        self._add_foreign_keys()

    def _existing_foreign_keys(self) -> set:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT constraint_name FROM information_schema.table_constraints "
                    "WHERE table_schema = :db AND constraint_type = 'FOREIGN KEY'"
                ),
                {"db": self.database},
            ).fetchall()
            return {r[0] for r in rows}

    def _add_foreign_keys(self):
        """Adds FK constraints (fact -> dim, dim -> dim) so ER-diagram tools
        like DBeaver can auto-draw the star schema's join lines. Idempotent:
        skips any constraint that already exists (checked by name via
        information_schema, since MySQL has no ADD CONSTRAINT IF NOT EXISTS)."""
        existing = self._existing_foreign_keys()
        with self.engine.connect() as conn:
            for constraint_name, child_table, child_col, parent_table, parent_col in FOREIGN_KEYS:
                if constraint_name in existing:
                    continue
                conn.execute(text(
                    f"ALTER TABLE `{child_table}` "
                    f"ADD CONSTRAINT `{constraint_name}` "
                    f"FOREIGN KEY (`{child_col}`) REFERENCES `{parent_table}` (`{parent_col}`)"
                ))
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
            # Dims and facts are reloaded independently and not always in FK
            # dependency order, and MySQL refuses to TRUNCATE a table that's
            # referenced by a live FK - so checks are turned off just for
            # this truncate+reload, then restored.
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            conn.execute(text(f"TRUNCATE TABLE `{table_name}`"))
            if not df.empty:
                df.to_sql(table_name, conn, if_exists="append", index=False)
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))

    def append_table(self, table_name: str, df: pd.DataFrame):
        if df.empty:
            return
        with self.engine.begin() as conn:
            conn.execute(text(DDL_STATEMENTS[table_name]))
            df.to_sql(table_name, conn, if_exists="append", index=False)
