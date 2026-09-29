"""
curated_to_gold.py
---------------------
Layer: CURATED -> GOLD (the star schema we designed: dim_* + fact_*)

Warehouse target: a MySQL database (default name "classicmodels_dw") in the
SAME Docker MySQL container used as the source, written to via
src/warehouse/mysql_manager.py - browsable directly in DBeaver.

SCD handling (per table_config.json -> "scd_type"):
    type1  -> dim_office, dim_product_line, dim_order_status
              simple full overwrite every run (small master data, no history needed)
    type2  -> dim_employee, dim_product, dim_customer
              row-hash change detection + expire-old/insert-new versioning,
              because credit_limit / sales rep / job title / price changes
              are business-meaningful history we must preserve

All the heavy lifting (reading curated Parquet, hashing rows, joining to
detect what changed) happens in PySpark, per the project requirement of
being PySpark end-to-end. The actual SCD2 NEW/CHANGED/UNCHANGED decision
per row is a tiny, pure, Spark-independent function (`classify_row`) so
it can be unit tested directly without spinning up a SparkSession.

Known local-project simplifications (documented, not hidden):
  - fact_order_line resolves customer/employee/product to whatever dim
    version is CURRENT at pipeline run time, not "as-of the order date".
    True as-of resolution would need row_effective_date/row_end_date
    range joins against order_date - left as a documented follow-up.
  - A natural key present in the current dim but missing from a fresh
    curated full-load (i.e. looks deleted at source) is left untouched
    (no hard delete, no auto-expire) - flagged as a WARNING log instead,
    since silently expiring on a MISSING row is a stronger assumption
    than this project needs today.
"""

import sys
import os
import hashlib
from datetime import datetime, timezone, date, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.utils.spark_utils import configure_pyspark_python
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.config_loader import load_secrets, resolve_path
from src.utils.logger import get_logger, ActivityLogger
from src.utils.metadata_manager import MetadataManager
from src.utils.constants import SCD2_OPEN_END_DATE
from src.warehouse.mysql_manager import MySQLManager
from src.utils.spark_utils import configure_pyspark_python

# ---------------------------------------------------------------------------
# Pure, Spark-independent SCD2 decision logic (unit tested in isolation)
# ---------------------------------------------------------------------------

def classify_row(in_current: bool, in_curated: bool, hash_matches: bool) -> str:
    """
    Given three booleans about ONE natural-key row, decide what the SCD2
    loader should do with it. Kept as a standalone function (no Spark, no
    DuckDB) so the branching logic itself can be tested with plain asserts.
    """
    if in_curated and not in_current:
        return "NEW"
    if in_curated and in_current and not hash_matches:
        return "CHANGED"
    if in_curated and in_current and hash_matches:
        return "UNCHANGED"
    if in_current and not in_curated:
        return "MISSING"
    return "UNKNOWN"


def compute_row_hash(*values) -> str:
    """MD5 hash of pipe-joined column values - used to detect if a
    dimension row changed since the last gold load. Pure function so it's
    trivially testable and reusable both in Spark UDFs and in pandas."""
    joined = "||".join("" if v is None else str(v) for v in values)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# dim_date - generated once, covers the full date range seen in the data
# ---------------------------------------------------------------------------

def build_dim_date_rows(min_date: date, max_date: date):
    rows = []
    current = min_date
    while current <= max_date:
        date_key = int(current.strftime("%Y%m%d"))
        rows.append({
            "date_key": date_key,
            "full_date": current,
            "day_name": current.strftime("%A"),
            "day_of_week": current.isoweekday(),
            "day_of_month": current.day,
            "week_of_year": current.isocalendar()[1],
            "month_num": current.month,
            "month_name": current.strftime("%B"),
            "quarter": (current.month - 1) // 3 + 1,
            "year": current.year,
            "is_weekend": current.isoweekday() in (6, 7),
        })
        current += timedelta(days=1)
    return rows


def load_dim_date(spark, dw: MySQLManager, curated_base: str, alog: ActivityLogger):
    activity = "gold_dim_date"
    alog.start(activity)
    try:
        orders = spark.read.parquet(os.path.join(curated_base, "orders"))
        payments = spark.read.parquet(os.path.join(curated_base, "payments"))

        all_dates = (
            orders.select(F.col("orderDate").alias("d"))
            .union(orders.select(F.col("requiredDate").alias("d")))
            .union(orders.select(F.col("shippedDate").alias("d")))
            .union(payments.select(F.col("paymentDate").alias("d")))
            .filter(F.col("d").isNotNull())
        )
        bounds = all_dates.agg(F.min("d").alias("min_d"), F.max("d").alias("max_d")).collect()[0]
        if bounds["min_d"] is None:
            alog.warning(activity, "No dates found in curated orders/payments - skipping dim_date")
            return

        rows = build_dim_date_rows(bounds["min_d"], bounds["max_d"])
        import pandas as pd
        df = pd.DataFrame(rows)
        dw.overwrite_table("dim_date", df)
        alog.success(activity, extra={"rows": len(df), "min_date": str(bounds["min_d"]), "max_date": str(bounds["max_d"])})
    except Exception as e:
        alog.failed(activity, error=e)
        raise


# ---------------------------------------------------------------------------
# Type 1 dimensions - simple full overwrite
# ---------------------------------------------------------------------------

def load_dim_office(spark, dw: MySQLManager, curated_base: str, alog: ActivityLogger):
    activity = "gold_dim_office"
    alog.start(activity)
    try:
        df = spark.read.parquet(os.path.join(curated_base, "offices"))
        # deterministic surrogate key: dense rank on the natural key
        from pyspark.sql.window import Window
        w = Window.orderBy("officeCode")
        out = df.withColumn("office_key", F.dense_rank().over(w)).select(
            "office_key",
            F.col("officeCode").alias("office_code"),
            F.col("city"),
            F.col("phone"),
            F.col("addressLine1").alias("address_line1"),
            F.col("addressLine2").alias("address_line2"),
            F.col("state"),
            F.col("country"),
            F.col("postalCode").alias("postal_code"),
            F.col("territory"),
        )
        pdf = out.toPandas()
        dw.overwrite_table("dim_office", pdf)
        alog.success(activity, extra={"rows": len(pdf)})
    except Exception as e:
        alog.failed(activity, error=e)
        raise


def load_dim_product_line(spark, dw: MySQLManager, curated_base: str, alog: ActivityLogger):
    activity = "gold_dim_product_line"
    alog.start(activity)
    try:
        from pyspark.sql.window import Window
        df = spark.read.parquet(os.path.join(curated_base, "productlines"))
        w = Window.orderBy("productLine")
        out = df.withColumn("product_line_key", F.dense_rank().over(w)).select(
            "product_line_key",
            F.col("productLine").alias("product_line"),
            F.col("textDescription").alias("text_description"),
        )
        pdf = out.toPandas()
        dw.overwrite_table("dim_product_line", pdf)
        alog.success(activity, extra={"rows": len(pdf)})
    except Exception as e:
        alog.failed(activity, error=e)
        raise


def load_dim_order_status(spark, dw: MySQLManager, curated_base: str, alog: ActivityLogger):
    activity = "gold_dim_order_status"
    alog.start(activity)
    try:
        from pyspark.sql.window import Window
        df = spark.read.parquet(os.path.join(curated_base, "orders")).select("status").distinct()
        w = Window.orderBy("status")
        out = df.withColumn("order_status_key", F.dense_rank().over(w)).select(
            "order_status_key",
            F.col("status").alias("status_code"),
        )
        pdf = out.toPandas()
        dw.overwrite_table("dim_order_status", pdf)
        alog.success(activity, extra={"rows": len(pdf)})
    except Exception as e:
        alog.failed(activity, error=e)
        raise


# ---------------------------------------------------------------------------
# Type 2 dimensions - hash-based change detection, expire-old/insert-new
# ---------------------------------------------------------------------------

def scd2_merge(curated_pdf, current_pdf, natural_key_col, surrogate_key_col, hash_cols, today):
    """
    Generic SCD2 merge, shared by dim_employee / dim_product / dim_customer.
    Pure pandas - no Spark, no DuckDB - so this is directly unit testable
    with plain DataFrames. Spark's job (reading curated Parquet) is done by
    the caller before this function is invoked; outrigger surrogate keys
    (office_key, product_line_key, sales_rep_employee_key) must already be
    resolved onto curated_pdf before calling this.

    curated_pdf      : pandas DataFrame, latest full snapshot from curated zone,
                        already renamed to gold column names (no surrogate key,
                        no row_hash/SCD columns yet)
    current_pdf      : pandas DataFrame, current gold dim table (empty on first run)
    natural_key_col  : natural key column name, present in curated_pdf
    surrogate_key_col: name of the surrogate key column in the gold dim (e.g. "employee_key")
    hash_cols        : list of curated_pdf business columns to hash for change detection
    today            : date this run is executing (used for row_effective_date/row_end_date)

    Returns: (result_df, n_new_or_changed, n_expired, n_missing)
    """
    curated_pdf = curated_pdf.copy()
    curated_pdf["row_hash"] = curated_pdf.apply(
        lambda row: compute_row_hash(*[row[c] for c in hash_cols]), axis=1
    )

    scd_cols = [surrogate_key_col, natural_key_col] + hash_cols + ["row_hash", "row_effective_date", "row_end_date", "is_current"]
    if current_pdf.empty:
        current_pdf = pd.DataFrame(columns=scd_cols)

    current_active = current_pdf[current_pdf["is_current"] == True] if not current_pdf.empty else current_pdf
    next_key = int(current_pdf[surrogate_key_col].max()) if len(current_pdf) > 0 else 0

    curated_keys = set(curated_pdf[natural_key_col])
    current_keys = set(current_active[natural_key_col]) if not current_active.empty else set()
    current_by_key = {row[natural_key_col]: row for _, row in current_active.iterrows()} if not current_active.empty else {}

    unchanged_rows, expired_rows, new_version_rows = [], [], []

    for _, row in curated_pdf.iterrows():
        nk = row[natural_key_col]
        in_current = nk in current_keys
        hash_matches = in_current and (current_by_key[nk]["row_hash"] == row["row_hash"])
        decision = classify_row(in_current=in_current, in_curated=True, hash_matches=hash_matches)

        if decision == "UNCHANGED":
            unchanged_rows.append(current_by_key[nk].to_dict())
        elif decision in ("NEW", "CHANGED"):
            if decision == "CHANGED":
                old_row = current_by_key[nk].to_dict()
                old_row["row_end_date"] = today - timedelta(days=1)
                old_row["is_current"] = False
                expired_rows.append(old_row)
            next_key += 1
            new_row = row.to_dict()
            new_row[surrogate_key_col] = next_key
            new_row["row_effective_date"] = today
            new_row["row_end_date"] = SCD2_OPEN_END_DATE
            new_row["is_current"] = True
            new_version_rows.append(new_row)

    # MISSING: natural key exists in current dim but absent from this
    # curated snapshot - left untouched (no hard delete/auto-expire),
    # caller logs a WARNING via the returned count.
    missing_keys = current_keys - curated_keys
    missing_rows = [current_by_key[k].to_dict() for k in missing_keys] if missing_keys else []

    touched_rows = unchanged_rows + expired_rows + new_version_rows + missing_rows
    touched_df = pd.DataFrame(touched_rows) if touched_rows else pd.DataFrame(columns=scd_cols)

    # historical (already is_current=False) rows from earlier runs must be preserved too
    historical_rows = current_pdf[current_pdf["is_current"] == False] if not current_pdf.empty else pd.DataFrame(columns=scd_cols)
    result = pd.concat([historical_rows, touched_df], ignore_index=True, sort=False)

    return result, len(new_version_rows), len(expired_rows), len(missing_rows)


def load_dim_employee(spark, dw: MySQLManager, curated_base: str, alog: ActivityLogger, today: date):
    activity = "gold_dim_employee"
    alog.start(activity)
    try:
        emp_pdf = spark.read.parquet(os.path.join(curated_base, "employees")).toPandas()
        office_pdf = dw.read_table("dim_office")

        if not office_pdf.empty:
            emp_pdf = emp_pdf.merge(
                office_pdf[["office_code", "office_key"]],
                left_on="officeCode", right_on="office_code", how="left",
            )
        else:
            emp_pdf["office_key"] = None

        curated_pdf = emp_pdf.rename(columns={
            "employeeNumber": "employee_number", "firstName": "first_name",
            "lastName": "last_name", "extension": "extension", "email": "email",
            "jobTitle": "job_title",
        })[["employee_number", "first_name", "last_name", "extension", "email", "office_key", "job_title"]]
        curated_pdf["employee_key"] = 0  # placeholder, assigned inside scd2_merge

        hash_cols = ["first_name", "last_name", "extension", "email", "office_key", "job_title"]
        current_pdf = dw.read_table("dim_employee")
        result, n_new, n_expired, n_missing = scd2_merge(
            curated_pdf, current_pdf, "employee_number", "employee_key", hash_cols, today
        )
        dw.overwrite_table("dim_employee", result)
        if n_missing:
            alog.warning(activity, f"{n_missing} employee(s) in gold missing from latest curated snapshot (left untouched)")
        alog.success(activity, extra={"new_or_changed_versions": n_new, "expired_versions": n_expired, "missing_from_source": n_missing})
    except Exception as e:
        alog.failed(activity, error=e)
        raise


def load_dim_product(spark, dw: MySQLManager, curated_base: str, alog: ActivityLogger, today: date):
    activity = "gold_dim_product"
    alog.start(activity)
    try:
        prod_pdf = spark.read.parquet(os.path.join(curated_base, "products")).toPandas()
        pl_pdf = dw.read_table("dim_product_line")

        if not pl_pdf.empty:
            prod_pdf = prod_pdf.merge(
                pl_pdf[["product_line", "product_line_key"]],
                left_on="productLine", right_on="product_line", how="left",
            )
        else:
            prod_pdf["product_line_key"] = None

        curated_pdf = prod_pdf.rename(columns={
            "productCode": "product_code", "productName": "product_name",
            "productScale": "product_scale", "productVendor": "product_vendor",
            "productDescription": "product_description", "MSRP": "msrp", "buyPrice": "buy_price",
        })[["product_code", "product_name", "product_line_key", "product_scale", "product_vendor",
            "product_description", "msrp", "buy_price"]]
        curated_pdf["product_key"] = 0

        hash_cols = ["product_name", "product_line_key", "product_scale", "product_vendor",
                     "product_description", "msrp", "buy_price"]
        current_pdf = dw.read_table("dim_product")
        result, n_new, n_expired, n_missing = scd2_merge(
            curated_pdf, current_pdf, "product_code", "product_key", hash_cols, today
        )
        dw.overwrite_table("dim_product", result)
        if n_missing:
            alog.warning(activity, f"{n_missing} product(s) in gold missing from latest curated snapshot (left untouched)")
        alog.success(activity, extra={"new_or_changed_versions": n_new, "expired_versions": n_expired, "missing_from_source": n_missing})
    except Exception as e:
        alog.failed(activity, error=e)
        raise


def load_dim_customer(spark, dw: MySQLManager, curated_base: str, alog: ActivityLogger, today: date):
    activity = "gold_dim_customer"
    alog.start(activity)
    try:
        cust_pdf = spark.read.parquet(os.path.join(curated_base, "customers")).toPandas()
        emp_pdf = dw.read_table("dim_employee")
        emp_current = emp_pdf[emp_pdf["is_current"] == True] if not emp_pdf.empty else emp_pdf

        if not emp_current.empty:
            cust_pdf = cust_pdf.merge(
                emp_current[["employee_number", "employee_key"]],
                left_on="salesRepEmployeeNumber", right_on="employee_number", how="left",
            )
            cust_pdf = cust_pdf.rename(columns={"employee_key": "sales_rep_employee_key"})
        else:
            cust_pdf["sales_rep_employee_key"] = None

        curated_pdf = cust_pdf.rename(columns={
            "customerNumber": "customer_number", "customerName": "customer_name",
            "contactLastName": "contact_last_name", "contactFirstName": "contact_first_name",
            "phone": "phone", "addressLine1": "address_line1", "addressLine2": "address_line2",
            "city": "city", "state": "state", "postalCode": "postal_code", "country": "country",
            "creditLimit": "credit_limit",
        })[["customer_number", "customer_name", "contact_last_name", "contact_first_name", "phone",
            "address_line1", "address_line2", "city", "state", "postal_code", "country",
            "sales_rep_employee_key", "credit_limit"]]
        curated_pdf["customer_key"] = 0

        hash_cols = ["customer_name", "contact_last_name", "contact_first_name", "phone",
                     "address_line1", "city", "state", "postal_code", "country",
                     "sales_rep_employee_key", "credit_limit"]
        current_pdf = dw.read_table("dim_customer")
        result, n_new, n_expired, n_missing = scd2_merge(
            curated_pdf, current_pdf, "customer_number", "customer_key", hash_cols, today
        )
        dw.overwrite_table("dim_customer", result)
        if n_missing:
            alog.warning(activity, f"{n_missing} customer(s) in gold missing from latest curated snapshot (left untouched)")
        alog.success(activity, extra={"new_or_changed_versions": n_new, "expired_versions": n_expired, "missing_from_source": n_missing})
    except Exception as e:
        alog.failed(activity, error=e)
        raise


# ---------------------------------------------------------------------------
# Fact tables
# ---------------------------------------------------------------------------

def load_fact_order_line(spark, dw: MySQLManager, curated_base: str, alog: ActivityLogger):
    activity = "gold_fact_order_line"
    alog.start(activity)
    try:
        orders = spark.read.parquet(os.path.join(curated_base, "orders"))
        orderdetails = spark.read.parquet(os.path.join(curated_base, "orderdetails"))

        dim_customer = dw.read_table("dim_customer")
        dim_customer = dim_customer[dim_customer["is_current"] == True] if not dim_customer.empty else dim_customer
        dim_product = dw.read_table("dim_product")
        dim_product = dim_product[dim_product["is_current"] == True] if not dim_product.empty else dim_product
        dim_status = dw.read_table("dim_order_status")

        joined = orderdetails.join(orders, on="orderNumber", how="inner")
        pdf = joined.toPandas()

        pdf["priceEach"] = pdf["priceEach"].astype(float)
        pdf["quantityOrdered"] = pdf["quantityOrdered"].astype(float)

        # sales_rep_employee_key on dim_customer already IS the resolved
        # dim_employee.employee_key (resolved once, at dim_customer build
        # time) - so fact_order_line.employee_key comes straight from this
        # one merge, no second join against dim_employee is needed.
        pdf = pdf.merge(dim_customer[["customer_number", "customer_key", "sales_rep_employee_key"]],
                         left_on="customerNumber", right_on="customer_number", how="left")
        pdf = pdf.rename(columns={"sales_rep_employee_key": "employee_key"})
        pdf = pdf.merge(dim_product[["product_code", "product_key", "buy_price", "msrp"]],
                         left_on="productCode", right_on="product_code", how="left")
        pdf = pdf.merge(dim_status[["status_code", "order_status_key"]],
                         left_on="status", right_on="status_code", how="left")
        pdf["buy_price"] = pdf["buy_price"].astype(float)          
        pdf["msrp"] = pdf["msrp"].astype(float) 

        pdf["order_date_key"] = pdf["orderDate"].apply(lambda d: int(d.strftime("%Y%m%d")) if pd_notna(d) else None)
        pdf["required_date_key"] = pdf["requiredDate"].apply(lambda d: int(d.strftime("%Y%m%d")) if pd_notna(d) else None)
        pdf["shipped_date_key"] = pdf["shippedDate"].apply(lambda d: int(d.strftime("%Y%m%d")) if pd_notna(d) else None)

        pdf["extended_sales_amount"] = pdf["quantityOrdered"] * pdf["priceEach"]
        pdf["extended_cost_amount"] = pdf["quantityOrdered"] * pdf["buy_price"]
        pdf["extended_margin_amount"] = pdf["extended_sales_amount"] - pdf["extended_cost_amount"]

        out = pdf.rename(columns={
            "orderNumber": "order_number",
            "orderLineNumber": "order_line_number",
            "quantityOrdered": "quantity_ordered",
            "priceEach": "price_each",
            "buy_price": "buy_price_at_order",
            "msrp": "msrp_at_order",
        })[[
            "order_date_key", "required_date_key", "shipped_date_key",
            "customer_key", "employee_key", "product_key", "order_status_key",
            "order_number", "order_line_number", "quantity_ordered", "price_each",
            "buy_price_at_order", "msrp_at_order",
            "extended_sales_amount", "extended_cost_amount", "extended_margin_amount",
        ]]

        dw.overwrite_table("fact_order_line", out)
        alog.success(activity, extra={"rows": len(out)})
    except Exception as e:
        alog.failed(activity, error=e)
        raise


def load_fact_payment(spark, dw: MySQLManager, curated_base: str, alog: ActivityLogger):
    activity = "gold_fact_payment"
    alog.start(activity)
    try:
        payments = spark.read.parquet(os.path.join(curated_base, "payments")).toPandas()

        dim_customer = dw.read_table("dim_customer")
        dim_customer = dim_customer[dim_customer["is_current"] == True] if not dim_customer.empty else dim_customer

        pdf = payments.merge(dim_customer[["customer_number", "customer_key"]],
                              left_on="customerNumber", right_on="customer_number", how="left")
        pdf["payment_date_key"] = pdf["paymentDate"].apply(lambda d: int(d.strftime("%Y%m%d")) if pd_notna(d) else None)

        out = pdf.rename(columns={
            "checkNumber": "check_number",
            "amount": "payment_amount",
        })[["payment_date_key", "customer_key", "check_number", "payment_amount"]]

        dw.overwrite_table("fact_payment", out)
        alog.success(activity, extra={"rows": len(out)})
    except Exception as e:
        alog.failed(activity, error=e)
        raise


def pd_notna(v):
    import pandas as pd
    return pd.notna(v)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_all():
    secrets = load_secrets()
    curated_base = resolve_path(secrets["paths"]["curated"])
    mysql_gold_cfg = secrets["warehouse"]["mysql"]

    logger = get_logger(log_dir=resolve_path(secrets["paths"]["logs"]))
    alog = ActivityLogger(logger, source="curated_to_gold")
    meta = MetadataManager(
        watermark_file=resolve_path(secrets["metadata"]["watermark_file"]),
        schema_registry_file=resolve_path(secrets["metadata"]["schema_registry_file"]),
        run_log_file=resolve_path(secrets["metadata"]["run_log_file"]),
    )
    configure_pyspark_python()
    spark_cfg = secrets["spark"]
    spark = (
        SparkSession.builder
        .appName(spark_cfg["app_name"] + "_gold")
        .master(spark_cfg["master"])
        .config("spark.sql.shuffle.partitions", spark_cfg["shuffle_partitions"])
        .getOrCreate()
    )

    dw = MySQLManager(mysql_gold_cfg)
    dw.ensure_schema()

    today = datetime.now(timezone.utc).date()
    steps = [
        ("dim_date", lambda: load_dim_date(spark, dw, curated_base, alog)),
        ("dim_office", lambda: load_dim_office(spark, dw, curated_base, alog)),
        ("dim_product_line", lambda: load_dim_product_line(spark, dw, curated_base, alog)),
        ("dim_order_status", lambda: load_dim_order_status(spark, dw, curated_base, alog)),
        ("dim_employee", lambda: load_dim_employee(spark, dw, curated_base, alog, today)),
        ("dim_product", lambda: load_dim_product(spark, dw, curated_base, alog, today)),
        ("dim_customer", lambda: load_dim_customer(spark, dw, curated_base, alog, today)),
        ("fact_order_line", lambda: load_fact_order_line(spark, dw, curated_base, alog)),
        ("fact_payment", lambda: load_fact_payment(spark, dw, curated_base, alog)),
    ]

    failures = []
    for name, step_fn in steps:
        start_time = datetime.now(timezone.utc)
        try:
            step_fn()
            meta.log_run(name, "gold", "SUCCESS", start_time=start_time)
        except Exception as e:
            meta.log_run(name, "gold", "FAILED", error_message=str(e), start_time=start_time)
            failures.append((name, str(e)))
            # dims must load before facts - if a dimension fails, stop rather
            # than building facts against a stale/incomplete dimension
            if name.startswith("dim_"):
                alog.failed("run_all", error=f"stopping: dimension '{name}' failed, skipping dependent facts")
                break
            continue

    spark.stop()

    if failures:
        alog.warning("run_all", f"{len(failures)} step(s) failed in gold load", extra={"failures": failures})
    else:
        alog.success("run_all", extra={"steps_completed": [s[0] for s in steps]})

    return failures


if __name__ == "__main__":
    run_all()
