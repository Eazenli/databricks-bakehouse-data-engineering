from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def create_empty_table(template_df: DataFrame, target_table: str) -> None:
    """Create an empty Delta table using the schema of a template DataFrame."""
    (
        template_df
        .limit(0)
        .write
        .format("delta")
        .mode("ignore")
        .saveAsTable(target_table)
    )

def ingest_bronze_dim_table(spark: SparkSession, source_table: str, target_table: str, source_name: str) -> None:
    """Inital load of a fixed reference table into the bronze layer"""

    bronze_df = (
        spark.table(source_table)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(source_name))
    )

    (bronze_df.write.format("delta").mode("overwrite").saveAsTable(target_table))

def ingest_bronze_fact_table_inc(
    df: DataFrame, 
    target_table: str, 
    source_name: str, 
    batch_id: str, 
    run_id: int) -> None:

    bronze_df = (
        df
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(source_name))
    )

    (bronze_df.write.format("delta").mode("append").saveAsTable(target_table))


