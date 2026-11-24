from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

### SILVER LAYER

def standardize_transactions(df:DataFrame) -> DataFrame :
    cols = ["transactionID", "customerID", "franchiseID", "dateTime", "product", "quantity", "unitPrice", "totalPrice", "_batch_id", "_run_id"]
    return (
        df.select(cols)
        .withColumn("transactionID", F.col("transactionID").cast("string"))
        .withColumn("customerID", F.col("customerID").cast("string"))
        .withColumn("franchiseID", F.col("franchiseID").cast("string"))
        .withColumn("transaction_date", F.col("dateTime").cast("date"))
        .withColumn("unitPrice", F.col("unitPrice").cast("decimal(10,2)"))
        .withColumn("totalPrice", F.col("totalPrice").cast("decimal(10,2)"))
        .withColumn("calculated_total",(F.col("unitPrice") * F.col("quantity")).cast("decimal(10,2)"))
    )
        
def standardize_customers(df:DataFrame) -> DataFrame :
    cols = ["customerID", "first_name", "last_name", "city", "state", "country", "continent", "gender"]
    return (
        df.select(cols)
        .withColumn("customerID", F.col("customerID").cast("string"))
        .withColumn("gender", F.lower(F.trim(F.col("gender"))))
    )

def standardize_franchises(df:DataFrame) -> DataFrame :
    cols = ["franchiseID", "name", "city", "district", "country", "size"]
    return (
        df.select(cols)
        .withColumn("franchiseID", F.col("franchiseID").cast("string"))
        .withColumnRenamed("name", "franchiseName")
    )


### GOLD LAYER

def enrich_with_franchises(df_transactions, df_franchises, cols_franchises:list):
    """
    Enrich the transactions dataframe with the franchise information for gold layer
    """
    return df_transactions.join(df_franchises.select(*cols_franchises), on="franchiseID", how="left")


def build_daily_sales_franchise_metrics(df:DataFrame) -> DataFrame:
    """
    Transform the transactions_franchises dataframe
    """
    daily_metrics = (
        df
        .groupBy("franchiseID", "franchiseName", "transaction_date")
        .agg(
            F.sum("totalPrice").alias("sum_revenue"),
            F.round(F.avg("totalPrice"), 2).alias("avg_revenue"),
            F.countDistinct("customerID").alias("amount_of_customers")
        )
    )
    ranking_window = (
        daily_metrics.withColumn(
            "rank_sum_revenue",
            F.dense_rank().over(Window.partitionBy("transaction_date").orderBy(F.desc("sum_revenue")))
        )
    )
    return ranking_window




