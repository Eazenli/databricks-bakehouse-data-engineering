from decimal import Decimal

from src.transformation import (
    standardize_transactions,
    standardize_customers,
    standardize_franchises,
)


def test_standardize_transactions_schema(transactions_df):
    result = standardize_transactions(transactions_df)
    dtypes = dict(result.dtypes)

    assert dtypes["transactionID"] == "string"
    assert dtypes["customerID"] == "string"
    assert dtypes["franchiseID"] == "string"
    assert dtypes["transaction_date"] == "date"
    assert dtypes["unitPrice"] == "decimal(10,2)"
    assert dtypes["totalPrice"] == "decimal(10,2)"
    assert dtypes["calculated_total"] == "decimal(10,2)"

    assert "_batch_id" in result.columns
    assert "_run_id" in result.columns


def test_standardize_transactions_calculated_total(transactions_df):
    result = standardize_transactions(transactions_df)

    row = (
        result
        .filter("transactionID = '1003103'")
        .first()
    )

    assert row["calculated_total"] == Decimal("84.00")


def test_standardize_customers(customers_df):
    result = standardize_customers(customers_df)
    dtypes = dict(result.dtypes)

    assert dtypes["customerID"] == "string"

    valid_gender = (
        result
        .filter("customerID = '2000075'")
        .first()["gender"]
    )

    assert valid_gender == "female"


def test_standardize_franchises(franchises_df):
    result = standardize_franchises(franchises_df)
    dtypes = dict(result.dtypes)

    assert dtypes["franchiseID"] == "string"
    assert "franchiseName" in result.columns
    assert "name" not in result.columns
