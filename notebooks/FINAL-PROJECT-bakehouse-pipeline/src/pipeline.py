
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType
from datetime import datetime

### AUDIT ###
AUDIT_SCHEMA = StructType([
    StructField("_run_id", IntegerType(), False),
    StructField("_batch_id", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("received_rows", IntegerType(), True),
    StructField("valid_rows", IntegerType(), True),
    StructField("invalid_rows", IntegerType(), True),
    StructField("removed_duplicates", IntegerType(), True),
    StructField("status", StringType(), False),
])

# Adding data into pipeline_audit table before ingesting the bronze inc table 

def start_audit(spark: SparkSession, run_id: int, batch_id: str, save_as: str) -> None:
    df= spark.createDataFrame(
        [(run_id,
          batch_id,
          datetime.now(),
          None,
          None,
          None,
          None,
          None,
          "RUNNING")],
        schema = AUDIT_SCHEMA
    )
    (df.write.format("delta").mode("append").saveAsTable(save_as))
    
# Update the audit table after Silver 
def end_audit(
    spark: SparkSession,
    audit_table: str, 
    run_id: int, 
    received_rows: int, 
    valid_rows: int,
    invalid_rows : int,
    removed_duplicates : int) -> None:

    audit_delta = DeltaTable.forName(spark, audit_table)

    audit_delta.update(
        condition = F.col("_run_id") == run_id,
        set = {
            "end_time" :F.current_timestamp(),
            "received_rows": F.lit(received_rows),
            "valid_rows": F.lit(valid_rows),
            "invalid_rows": F.lit(invalid_rows),
            "removed_duplicates": F.lit(removed_duplicates),
            "status": F.lit("SUCCESS")
        }
    )

# Prevent FAIL for audit 
def fail_audit(spark: SparkSession, audit_table: str, run_id:int) -> None:
    audit_delta = DeltaTable.forName(spark, audit_table)

    audit_delta.update(
        condition=F.col("_run_id") == run_id,
        set={
            "end_time": F.current_timestamp(),
            "status": F.lit("FAIL")
        }
    )

def get_next_run_id(spark:SparkSession, audit_df: str) -> int:
    last_run_id = (
        spark.read.table(audit_df)
        .agg(F.max("_run_id"))
        .first()[0]
    )

    if last_run_id is None:
        return 1
    else:
        return last_run_id + 1                                                                                 


# Apply insert-only MERGE to Silver layer
def merge_silver_inc(spark: SparkSession, df: DataFrame, silver_table: str) -> None:
    silver_delta = DeltaTable.forName(spark, silver_table)
    (
        silver_delta.alias("target")
        .merge(
            df.alias("source"),
            "source.transactionID = target.transactionID",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    
def write_rejects(df: DataFrame,target_table: str) -> None:
    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )

def write_gold_inc(df: DataFrame, save_as: str) -> None:
    (
        df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(save_as)
    )

BUSINESS_COLUMNS = ["transactionID", "customerID", "franchiseID", "dateTime", "product", "quantity", "unitPrice", "totalPrice"]

def process_batch(
    spark: SparkSession, 
    batch_df:DataFrame, 
    batch_id: str, 
    config:PipelineConfig) -> None:

    # 1.New execution for new run_id 
    run_id = get_next_run_id(config.pipeline_audit)
    print(f"Starting {batch_id} - run_id={run_id}")

    # 2. Start audit 
    start_audit(
        run_id=run_id,
        batch_id=batch_id,
        save_as=config.pipeline_audit
        )
    
    try:
        
        # 3. Ingest bronze 
        print("Ingest Bronze...")
        ingest_bronze_fact_table_inc(
            df=batch_df,
            target_table=config.bronze_transactions,
            source_name=config.source_transactions,
            batch_id=batch_id,
            run_id=run_id,
        )
        # 4. Read Bronze
        bronze_run_df = (
        spark.read.table(config.bronze_transactions)
        .filter(F.col("_run_id") == run_id)
        )

        # 5. Silver : standarization
        print("Processing Silver...")
        std_transactions = standardize_transactions(bronze_run_df)
        # 6. Silver : deduplication
        dedup_transactions, quality = check_global_quality(
            std_transactions,
            "transactionID"
        )
        # 7.Silver : validation
        customers_dim = spark.read.table(config.silver_customers)
        franchises_dim = spark.read.table(config.silver_franchises)

        checked_transactions = validate_transactions(dedup_transactions, customers_dim, franchises_dim)
        # 8. Silver: Split valid and invalid rows
        valid_transa_df, invalid_transa_df = split_valid_invalid(checked_transactions)

        # 9. Silver : Check conflict between new and existing records
        check_silver_conflict(
            spark=spark,
            incoming_df=valid_transa_df,
            silver_table=config.silver_transactions,
            key="transactionID",
            business_cols=BUSINESS_COLUMNS
        )
        
        # 10. Silver : Merge valid rows and write invalid rows 
        merge_silver_inc(
            spark=spark,
            df=valid_transa_df,
            silver_table=config.silver_transactions
        )
        write_rejects(df=invalid_transa_df, save_as=config.silver_invalid_transactions)

        # 11. Build + write Gold
        silver_transa_df = spark.read.table(config.silver_transactions)
        print("Building Gold...")
        transactions_franchises_df= enrich_with_franchises(
            silver_transa_df, 
            franchises_dim, 
            cols_franchises=["franchiseID", "franchiseName", "city", "district", "country", "size"]
        )

        gold_df = build_daily_sales_franchise_metrics(transactions_franchises_df)

        write_gold_inc(gold_df, config.gold_daily_franchise_sales)

        # 11. Update audit table
        print("Updating audit...")
        received_rows = bronze_run_df.count()
        valid_rows = valid_transa_df.count()
        invalid_rows = invalid_transa_df.count()
        removed_duplicates = quality["removed_duplicates"]

        end_audit(
        audit_table=pipeline_audit, 
        run_id=run_id, 
        received_rows=received_rows,
        valid_rows=valid_rows,
        invalid_rows=invalid_rows,
        removed_duplicates=removed_duplicates
        )

        print(
        f"""{batch_id} completed - run_id={run_id}
        received  : {received_rows}
        valid     : {valid_rows}
        rejected  : {invalid_rows}
        duplicates: {removed_duplicates}
        """
        )
    except Exception as e:

            fail_audit(
                audit_table=pipeline_audit,
                run_id=run_id
            )

            print(f"{batch_id} failed - run_id={run_id}")
            print(f"Error: {e}")

            raise