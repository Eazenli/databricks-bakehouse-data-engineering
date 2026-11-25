from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str = "workspace"
    schema: str = "bakehouse_training"

    # Source tables
    source_transactions: str = "samples.bakehouse.sales_transactions"
    source_customers: str = "samples.bakehouse.sales_customers"
    source_franchises: str = "samples.bakehouse.sales_franchises"

    @property
    def bronze_transactions(self) -> str:
        return f"{self.catalog}.{self.schema}.bronze_transactions_fact_inc"

    @property
    def bronze_customers(self) -> str:
        return f"{self.catalog}.{self.schema}.bronze_customers_dim"

    @property
    def bronze_franchises(self) -> str:
        return f"{self.catalog}.{self.schema}.bronze_franchises_dim"

    @property
    def silver_transactions(self) -> str:
        return f"{self.catalog}.{self.schema}.silver_transactions_fact_inc"

    @property
    def silver_invalid_transactions(self) -> str:
        return (
            f"{self.catalog}.{self.schema}."
            "silver_invalid_transactions_fact_inc"
        )

    @property
    def silver_customers(self) -> str:
        return f"{self.catalog}.{self.schema}.silver_customers_dim"

    @property
    def silver_franchises(self) -> str:
        return f"{self.catalog}.{self.schema}.silver_franchises_dim"

    @property
    def gold_daily_franchise_sales(self) -> str:
        return (
            f"{self.catalog}.{self.schema}."
            "gold_daily_franchise_sales_inc"
        )

    @property
    def pipeline_audit(self) -> str:
        return f"{self.catalog}.{self.schema}.pipeline_audit"
    

config = PipelineConfig()

