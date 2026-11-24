from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame, Column

def check_global_quality(df:DataFrame, key:str) -> tuple[DataFrame, dict] :
    global_quality = {
        "null_key": (df.filter(F.col(key).isNull()).count()),
        "identical_rows":(
            df.groupBy(*df.columns).count().filter(F.col("count") > 1).count()
        ),
        "removed_duplicates": 0
    }
    print(global_quality)

    if global_quality["identical_rows"] > 0:
        print("There are duplicate rows in the dataset")

        dup_rows = df.groupBy(*df.columns).count().filter(F.col("count") > 1)

        df_dedup = df.dropDuplicates()
        dedup_rows = df_dedup.count()

        input_rows = df.count()
        
        removed_dup_rows = dup_rows.select(F.sum(F.col("count") -1).alias("removed")).first()["removed"]
        global_quality["removed_duplicates"] = removed_dup_rows

        assert input_rows == dedup_rows + removed_dup_rows
    else:
        df_dedup = df

    # Check if there are different rows sharing the same key after deduplication the identical rows
    dup_keys_rows = df_dedup.groupBy(key).count().filter(F.col("count") > 1)
    if dup_keys_rows.count() > 0:
        raise ValueError(f"There are different rows sharing the same {key}")
    else:
        print(f"No diffrent rows sharing the same {key} after checking the identical duplicates.")

    return df_dedup, global_quality
    
def is_missing(col_name:str) -> Column:
    return (
        F.when(
            F.col(col_name).isNull() | (F.trim(F.col(col_name)) == ""),
            F.lit(f"missing_{col_name}")
        )
    )

def validate_customers(df):
    return(
        df
        .withColumn(
            "dq_reason",
            F.coalesce(
                is_missing("customerID"),
                is_missing("first_name"),
                is_missing("last_name"),
                is_missing("city"),
                is_missing("state"),
                is_missing("country"),

                F.when(
                    F.col("continent").isNotNull() & 
                    ~F.initcap(F.col("continent")).isin(
                        "Africa",
                        "Asia",
                        "Europe",
                        "North America",
                        "South America",
                        "Oceania",
                        "Antarctica"
                    ),
                    F.lit("invalid_continent")
                )
                .when(
                    F.col("gender").isNotNull() & 
                    ~F.lower(F.col("gender")).isin("female","male"),
                    F.lit("invalid_gender")
                )
            )
        )
        .withColumn(
            "dq_status",
            F.when(
                F.col("dq_reason").isNull(),
                F.lit("VALID")
            ).otherwise(F.lit("INVALID"))
        )
        .withColumn(
            "checked_at",
            F.current_timestamp()
        )
    )

def validate_franchises(df):
    return(
        df
        .withColumn(
            "dq_reason",
           F.coalesce(
                is_missing("franchiseID"),
                is_missing("franchiseName"),
                is_missing("city"),
                is_missing("district"),
                is_missing("country"),
                F.when(
                    F.col("size").isNotNull() & 
                    ~F.col("size").isin("S", "M", "L", "XL", "XXL"),
                    F.lit("invalid_size")
                )
            )   
        )
        .withColumn(
            "dq_status",
            F.when(
                F.col("dq_reason").isNull(),
                F.lit("VALID")
            ).otherwise(F.lit("INVALID"))
        )
        .withColumn(
            "checked_at",
            F.current_timestamp()
        )
    )

def validate_transactions(df_dedup, df_customers, df_franchises, tolerance=0.01):
    return(
        df_dedup
        .join(
            df_customers.select("customerID").distinct().withColumn("customer_exists", F.lit(True)),
            on="customerID",
            how="left"
        )
        .join(
            df_franchises.select("franchiseID").distinct().withColumn("franchise_exists", F.lit(True)),
            on="franchiseID",
            how="left"
        )
        .withColumn(
            "dq_reason",
            # Required keys and product name 
            F.coalesce(
                is_missing("transactionID"),
                is_missing("customerID"),
                is_missing("franchiseID"),
                is_missing("product"), 
            # Date must be convertible and non-null
                F.when(F.col("dateTime").isNull()
                    | F.col("transaction_date").isNull(),
                    F.lit("invalid_date")
                )
                # Quantity > 0
                .when(F.col("quantity").isNull()
                    | (F.col("quantity") <= 0),
                    F.lit("invalid_quantity")
                )
                # Prices >= 0
                .when(F.col("unitPrice").isNull()
                    | (F.col("unitPrice") < 0)
                    | F.col("totalPrice").isNull()
                    | (F.col("totalPrice") < 0),
                    F.lit("invalid_price")
                )
                # Difference greater than tolerance
                .when(F.abs(
                        F.col("totalPrice")
                        - F.col("calculated_total")
                    ) > tolerance,
                    F.lit("price_mismatch")
                )
                # Unknown customers and unknown franchises
                .when(
                    F.col("customer_exists").isNull(),
                    F.lit("unknown_customer")
                )
                .when(
                    F.col("franchise_exists").isNull(),
                    F.lit("unknown_franchise")
                ).otherwise(F.lit(None))
            )
        )
        .withColumn(
            "dq_status",
            F.when(
                F.col("dq_reason").isNull(),
                F.lit("VALID")
            ).otherwise(F.lit("INVALID"))
        )
        .withColumn(
            "checked_at",
            F.current_timestamp()
        )
    )

def split_valid_invalid(df:DataFrame) -> tuple[DataFrame, DataFrame]:
    valid_df = df.filter(F.col("dq_status") == "VALID")
    invalid_df = df.filter(F.col("dq_status") == "INVALID")

    return valid_df, invalid_df

def check_join_cardinality(before_df: DataFrame, after_df: DataFrame) -> None:

    before_count = before_df.count()
    after_count = after_df.count()

    if before_count != after_count:
        raise ValueError(
            "Join changed transaction cardinality: "
            f"{before_count} -> {after_count}"
        )

def check_silver_conflict(spark: SparkSession, incoming_df: DataFrame, silver_table, key: str, business_cols: list[str]) -> None:
    """
    Since insert-only MERGE check the new records on key "transactionID", what if the new record has same key but different business values ? P.S.: In general, new record having same key but different business values is a UPDATE, but in this case, it"s a conflict. -> raise ValueError
    """
    
    existing_df = spark.read.table(silver_table)

    existing_incoming_joined_df = (
        incoming_df.alias("incoming")
        .join(
            existing_df.alias("existing"), 
            on=key, 
            how="inner"
        )
    )
    conflicts = None
    for col in business_cols:
        difference = ~F.col(f"incoming.{col}").eqNullSafe(F.col(f"existing.{col}"))
        if conflicts is None:
            conflicts = difference
        else:
            conflicts = conflicts | difference
    
    conflict_df = existing_incoming_joined_df.filter(conflicts)

    if conflict_df.count()>0 :
        raise ValueError(f"Conflicting business data found for existing {key}")

def check_idempotenc_after_replay(
    before_df: DataFrame,
    after_df: DataFrame,
    key: str | list[str],
    revenue_col: str,
    layer: str
) -> None:

    if isinstance(key, str):
        key = [key]

    # 1. Row count
    assert before_df.count() == after_df.count()

    # 2. Same contents in both directions
    assert before_df.exceptAll(after_df).count() == 0
    assert after_df.exceptAll(before_df).count() == 0

    # 3. Key uniqueness
    duplicate_keys = (
        after_df
        .groupBy(*key)
        .count()
        .filter(F.col("count") > 1)
    )
    assert duplicate_keys.count() == 0

    # 4. Same revenue
    revenue_before = (
        before_df
        .agg(F.sum(revenue_col).alias("revenue"))
        .first()["revenue"]
    )

    revenue_after = (
        after_df
        .agg(F.sum(revenue_col).alias("revenue"))
        .first()["revenue"]
    )

    assert revenue_before == revenue_after

    print(f"Idempotence checks for {layer} passed")