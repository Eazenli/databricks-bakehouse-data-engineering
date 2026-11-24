import pytest

from src.transformation import (
    standardize_transactions,
    standardize_customers,
    standardize_franchises,
)
from src.quality import (
    check_global_quality,
    validate_customers,
    validate_franchises,
    validate_transactions,
    split_valid_invalid,
)


def test_transaction_deduplication(transactions_df):
    standardized = standardize_transactions(transactions_df)

    dedup_df, quality = check_global_quality(
        standardized,
        "transactionID",
    )

    assert standardized.count() == 12
    assert dedup_df.count() == 11
    assert quality["identical_rows"] == 1
    assert quality["removed_duplicates"] == 1


def test_customer_deduplication(customers_df):
    standardized = standardize_customers(customers_df)

    dedup_df, quality = check_global_quality(
        standardized,
        "customerID",
    )

    assert standardized.count() == 11
    assert dedup_df.count() == 10
    assert quality["identical_rows"] == 1
    assert quality["removed_duplicates"] == 1


def test_franchise_deduplication(franchises_df):
    standardized = standardize_franchises(franchises_df)

    dedup_df, quality = check_global_quality(
        standardized,
        "franchiseID",
    )

    assert standardized.count() == 9
    assert dedup_df.count() == 8
    assert quality["identical_rows"] == 1
    assert quality["removed_duplicates"] == 1


def test_customer_validation(customers_df):
    standardized = standardize_customers(customers_df)
    dedup_df, _ = check_global_quality(
        standardized,
        "customerID",
    )

    checked = validate_customers(dedup_df)
    valid_df, invalid_df = split_valid_invalid(checked)

    assert valid_df.count() == 2
    assert invalid_df.count() == 8
    assert checked.count() == 10


def test_customer_validation_reasons(customers_df):
    standardized = standardize_customers(customers_df)
    dedup_df, _ = check_global_quality(
        standardized,
        "customerID",
    )

    checked = validate_customers(dedup_df)

    reasons = {
        row["dq_reason"]
        for row in checked.select("dq_reason").collect()
        if row["dq_reason"] is not None
    }

    assert "missing_customerID" in reasons
    assert "missing_first_name" in reasons
    assert "missing_last_name" in reasons
    assert "missing_city" in reasons
    assert "missing_state" in reasons
    assert "missing_country" in reasons
    assert "invalid_continent" in reasons
    assert "invalid_gender" in reasons


def test_franchise_validation(franchises_df):
    standardized = standardize_franchises(franchises_df)
    dedup_df, _ = check_global_quality(
        standardized,
        "franchiseID",
    )

    checked = validate_franchises(dedup_df)
    valid_df, invalid_df = split_valid_invalid(checked)

    assert valid_df.count() == 2
    assert invalid_df.count() == 6
    assert checked.count() == 8


def test_franchise_validation_reasons(franchises_df):
    standardized = standardize_franchises(franchises_df)
    dedup_df, _ = check_global_quality(
        standardized,
        "franchiseID",
    )

    checked = validate_franchises(dedup_df)

    reasons = {
        row["dq_reason"]
        for row in checked.select("dq_reason").collect()
        if row["dq_reason"] is not None
    }
    

    assert "missing_franchiseID" in reasons
    assert "missing_franchiseName" in reasons
    assert "missing_city" in reasons
    assert "missing_district" in reasons
    assert "missing_country" in reasons
    assert "invalid_size" in reasons


def test_transaction_validation(
    transactions_df,
    customers_df,
    franchises_df,
):
    transactions_std = standardize_transactions(transactions_df)
    transactions_dedup, _ = check_global_quality(
        transactions_std,
        "transactionID",
    )

    customers_std = standardize_customers(customers_df)
    customers_dedup, _ = check_global_quality(
        customers_std,
        "customerID",
    )
    customers_checked = validate_customers(customers_dedup)
    valid_customers, _ = split_valid_invalid(customers_checked)

    franchises_std = standardize_franchises(franchises_df)
    franchises_dedup, _ = check_global_quality(
        franchises_std,
        "franchiseID",
    )
    franchises_checked = validate_franchises(franchises_dedup)
    valid_franchises, _ = split_valid_invalid(franchises_checked)

    checked = validate_transactions(
        transactions_dedup,
        valid_customers,
        valid_franchises,
    )

    valid_df, invalid_df = split_valid_invalid(checked)

    assert valid_df.count() == 2
    assert invalid_df.count() == 9
    assert checked.count() == 11


def test_transaction_validation_reasons(
    transactions_df,
    customers_df,
    franchises_df,
):
    transactions_std = standardize_transactions(transactions_df)
    transactions_dedup, _ = check_global_quality(
        transactions_std,
        "transactionID",
    )

    customers_std = standardize_customers(customers_df)
    customers_dedup, _ = check_global_quality(
        customers_std,
        "customerID",
    )
    valid_customers, _ = split_valid_invalid(
        validate_customers(customers_dedup)
    )

    franchises_std = standardize_franchises(franchises_df)
    franchises_dedup, _ = check_global_quality(
        franchises_std,
        "franchiseID",
    )
    valid_franchises, _ = split_valid_invalid(
        validate_franchises(franchises_dedup)
    )

    checked = validate_transactions(
        transactions_dedup,
        valid_customers,
        valid_franchises,
    )

    reasons = {
        row["dq_reason"]
        for row in checked.select("dq_reason").collect()
        if row["dq_reason"] is not None
    }

    assert "missing_transactionID" in reasons
    assert "missing_product" in reasons
    assert "invalid_date" in reasons
    assert "invalid_quantity" in reasons
    assert "invalid_price" in reasons
    assert "price_mismatch" in reasons


def test_different_rows_with_same_key_raise_error(spark):
    df = spark.createDataFrame(
        [
            ("T001", "product_a"),
            ("T001", "product_b"),
        ],
        ["transactionID", "product"],
    )

    with pytest.raises(ValueError):
        check_global_quality(df, "transactionID")
