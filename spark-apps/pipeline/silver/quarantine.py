from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def apply_rules_and_split(
    df: DataFrame,
    rules: dict[str, str],
    source_name: str,
) -> tuple[DataFrame, DataFrame]:
    """
    Evaluates quality rules on df and splits it into:
    - df_valid:      rows that pass every rule (is_quarantined=False).
    - df_quarantine: rows that fail at least one rule, with a 'failed_rules'
                     array column listing which rules failed.
    """
    for name, constraint in rules.items():
        df = df.withColumn(f"_rule_{name}", F.expr(constraint))

    rule_cols = [F.col(f"_rule_{name}") for name in rules]
    all_pass = rule_cols[0]
    for c in rule_cols[1:]:
        all_pass = all_pass & c
    df = df.withColumn("is_quarantined", ~all_pass)

    # Build array of failed rule names; array_compact removes the nulls for passing rules.
    failed_array = F.array(*[
        F.when(~F.col(f"_rule_{name}"), F.lit(name))
        for name in rules
    ])
    df = df.withColumn("failed_rules", F.array_compact(failed_array))
    df = df.drop(*[f"_rule_{name}" for name in rules])

    df_valid = (
        df.filter(~F.col("is_quarantined"))
        .drop("is_quarantined", "failed_rules")
    )
    df_quarantine = (
        df.filter(F.col("is_quarantined"))
        .drop("is_quarantined")
    )
    return df_valid, df_quarantine
