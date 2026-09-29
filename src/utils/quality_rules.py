"""
quality_rules.py
-------------------
Config-driven data-quality rule engine for the Raw -> Curated layer.

    not_null        {"type": "not_null", "columns": [...]}
    min_value       {"type": "min_value", "column": "...", "value": N}
                     (nulls pass this rule - use not_null for null-checks)
    max_value       {"type": "max_value", "column": "...", "value": N}
    allowed_values  {"type": "allowed_values", "column": "...", "values": [...]}

A row that fails ANY rule (or is a duplicate PK) is quarantined with a
"quarantine_reason" column listing every rule it broke, semicolon-joined
- so an invalid row is never silently dropped and its exact reason is
always visible for debugging (or reprocessing) later.
"""

import json
import os

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def load_quality_rules(path: str) -> dict:
    """Returns {} (no rules for any table) if the file doesn't exist yet -
    that degrades to duplicate-PK-checking only, never crashes."""
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        return json.load(f)


def _rule_condition(rule: dict):
    """Returns (spark_boolean_condition_that_is_TRUE_on_violation, reason_label)."""
    rtype = rule["type"]

    if rtype == "not_null":
        cond = None
        for c in rule["columns"]:
            f = F.col(c).isNull()
            cond = f if cond is None else (cond | f)
        return cond, f"not_null[{','.join(rule['columns'])}]"

    elif rtype == "min_value":
        c, v = rule["column"], rule["value"]
        return (F.col(c).isNotNull() & (F.col(c) < F.lit(v))), f"min_value[{c}<{v}]"

    elif rtype == "max_value":
        c, v = rule["column"], rule["value"]
        return (F.col(c).isNotNull() & (F.col(c) > F.lit(v))), f"max_value[{c}>{v}]"

    elif rtype == "allowed_values":
        c, vals = rule["column"], rule["values"]
        return (F.col(c).isNotNull() & (~F.col(c).isin(vals))), f"allowed_values[{c}]"

    else:
        raise ValueError(f"Unknown quality rule type in quality_rules.json: '{rtype}'")


def apply_quality_rules(df, rules: list, primary_key_cols: list):
    """
    Evaluates every rule in `rules` plus a built-in duplicate-primary-key
    check against `df`.

    Returns (valid_df, invalid_df):
      - valid_df   : rows that passed every rule, original columns only.
      - invalid_df : rows that broke at least one rule, original columns
                     + a "quarantine_reason" column (semicolon-joined list
                     of every broken rule, e.g. "not_null[email]; duplicate_primary_key").

    Duplicate handling: the FIRST occurrence of a primary key (by original
    row order) is treated as the valid one; every later occurrence is
    quarantined as "duplicate_primary_key" rather than silently vanishing
    via dropDuplicates().
    """
    working = df
    reason_cols = []

    for i, rule in enumerate(rules):
        cond, label = _rule_condition(rule)
        col_name = f"_reason_{i}"
        working = working.withColumn(col_name, F.when(cond, F.lit(label)))
        reason_cols.append(col_name)

    # Built-in duplicate primary-key check - always runs, not config-gated.
    w = Window.partitionBy(*primary_key_cols).orderBy(F.monotonically_increasing_id())
    working = working.withColumn("_row_num", F.row_number().over(w))
    working = working.withColumn(
        "_reason_dup",
        F.when(F.col("_row_num") > 1, F.lit("duplicate_primary_key")),
    )
    reason_cols.append("_reason_dup")

    working = working.withColumn(
        "quarantine_reason",
        F.array_join(
            F.array_except(
                F.array(*[F.col(c) for c in reason_cols]),
                F.array(F.lit(None).cast("string")),
            ),
            "; ",
        ),
    )

    invalid_df = working.filter(F.length("quarantine_reason") > 0).drop(*reason_cols, "_row_num")
    valid_df = working.filter(F.length("quarantine_reason") == 0).drop(*reason_cols, "_row_num", "quarantine_reason")

    return valid_df, invalid_df
