import pytest
from datetime import datetime
from decimal import Decimal
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    active_session = SparkSession.getActiveSession()

    if active_session is not None:
        yield active_session
        return

    local_spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("bakehouse-ci-tests")
        .getOrCreate()
    )

    yield local_spark
    local_spark.stop()


@pytest.fixture
def transactions_df(spark):
    data = [
        # 3 initially valid rows
        (1003103, 2000075, 3000047, datetime(2024, 5, 4, 23, 44), "Pearly Pies", 28, Decimal("3.00"), Decimal("84.00"), "test_batch", 1),

        (1000864, 2000290, 3000047, datetime(2024, 5, 6, 20, 46), "Golden Gate Ginger", 4, Decimal("3.00"), Decimal("12.00"), "test_batch", 1),

        (1003222, 2000179, 3000046, datetime(2024, 5, 8, 10, 19), "Outback Oatmeal", 36, Decimal("3.00"), Decimal("108.00"),"test_batch", 1),

        # Strict duplicate of first row
        (1003103, 2000075, 3000047, datetime(2024, 5, 4, 23, 44), "Pearly Pies", 28, Decimal("3.00"), Decimal("84.00"), "test_batch", 1),

        # Invalid date
        (1003230, 2000248, 3000046, None, "Tokyo Tidbits", 54, Decimal("3.00"), Decimal("162.00"),"test_batch", 1),

        # Empty product
        (1004001, 2000179, 3000046, datetime(2024, 5, 8, 10, 19), "", 10, Decimal("3.00"), Decimal("30.00"),"test_batch", 1),

        # quantity = 0
        (1004002, 2000179, 3000046, datetime(2024, 5, 8, 11, 20), "Pearly Pies", 0, Decimal("3.00"), Decimal("0.00"), "test_batch", 1),

        # Price mismatch
        (1004005, 2000179, 3000046, datetime(2024, 5, 8, 14, 20), "Tokyo Tidbits", 2, Decimal("3.00"), Decimal("10.00"), "test_batch", 1),

        # Negative price
        (1004004, 2000179, 3000046, datetime(2024, 5, 8, 13, 20), "Outback Oatmeal", 2, Decimal("-3.00"), Decimal("-6.00"), "test_batch", 1),

        # Missing transaction ID
        (None, 2000179, 3000046, datetime(2024, 5, 8, 15, 20), "Pearly Pies", 2, Decimal("3.00"), Decimal("6.00"), "test_batch", 1),

        # Unknown customer
        (1004006, 9999999, 3000046, datetime(2024, 5, 8, 16, 20), "Pearly Pies", 2, Decimal("3.00"), Decimal("6.00"), "test_batch", 1),

        # Unknown franchise
        (1004007, 2000179, 9999999, datetime(2024, 5, 8, 17, 20), "Pearly Pies", 2, Decimal("3.00"), Decimal("6.00"), "test_batch", 1),
    ]

    columns = [
        "transactionID",
        "customerID",
        "franchiseID",
        "dateTime",
        "product",
        "quantity",
        "unitPrice",
        "totalPrice",
        "_batch_id",
        "_run_id",
    ]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def customers_df(spark):
    data = [
        ("2000075", "Alice", "Martin", "Paris", "Ile-de-France", "France", "Europe", "female"),
        ("2000290", "John", "Smith", "Boston", "Massachusetts", "US", "North America", "male"),

        # Strict duplicate
        ("2000075", "Alice", "Martin", "Paris", "Ile-de-France", "France", "Europe", "female"),

        (None, "Emma", "Brown", "London", "England", "UK", "Europe", "female"),
        ("2000248", "", "Garcia", "Madrid", "Madrid", "Spain", "Europe", "female"),
        ("2000179", "Lucas", None, "Lyon", "Auvergne-Rhone-Alpes", "France", "Europe", "male"),
        ("2000779", "Anna", "Schmidt", "", "Bavaria", "Germany", "Europe", "female"),
        ("2000279", "Kenji", "Tanaka", "Tokyo", None, "Japan", "Asia", "male"),
        ("2000275", "Maria", "Silva", "Lisbon", "Lisbon", "", "Europe", "female"),
        ("2000375", "Paul", "Wilson", "Chicago", "Illinois", "US", "Mars", "male"),
        ("2000475", "Sophie", "Bernard", "Paris", "Ile-de-France", "France", "Europe", "unknown"),
    ]

    columns = [
        "customerID",
        "first_name",
        "last_name",
        "city",
        "state",
        "country",
        "continent",
        "gender",
    ]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def franchises_df(spark):
    data = [
        ("3000047", "Golden Crumbs", "San Francisco", "Mission", "US", "XL"),
        ("3000121", "Tokyo Treats", "Tokyo", "Shibuya", "Japan", "M"),

        # Strict duplicate
        ("3000047", "Golden Crumbs", "San Francisco", "Mission", "US", "XL"),

        (None, "Sugar Rush", "Melbourne", "Fitzroy", "Australia", "L"),
        ("3000046", "", "Sydney", "Bondi Beach", "Australia", "L"),
        ("3000042", "Sweet Dreams", "", "Wicker Park", "US", "XXL"),
        ("3000145", "Kyoto Kravings", "Kyoto", None, "Japan", "S"),
        ("3000178", "The Baking Lab", "Perth", "Leederville", "", "L"),
        ("3000695", "Crumbly Delights", "Brisbane", "West End", "Australia", "HUGE"),
    ]

    columns = [
        "franchiseID",
        "name",
        "city",
        "district",
        "country",
        "size",
    ]

    return spark.createDataFrame(data, columns)
