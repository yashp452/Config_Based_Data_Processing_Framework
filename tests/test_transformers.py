import pytest
from pyspark.sql import Row

from src.transformers.cast_types import CastTypesTransformer
from src.transformers.drop_duplicates import DropDuplicatesTransformer
from src.transformers.factory import TransformerFactory
from src.transformers.filter_nulls import FilterNullsTransformer


# ---------------------------------------------------------------------------
# cast_types
# ---------------------------------------------------------------------------

def test_cast_types_single_column(spark):
    df = spark.createDataFrame([Row(id=1, amount="9.99")])
    result = CastTypesTransformer({"columns": {"amount": "double"}}).transform(df)
    assert result.schema["amount"].dataType.simpleString() == "double"


def test_cast_types_multiple_columns(spark):
    df = spark.createDataFrame([Row(qty="3", price="12.50", flag="1")])
    result = CastTypesTransformer(
        {"columns": {"qty": "integer", "price": "double", "flag": "boolean"}}
    ).transform(df)
    assert result.schema["qty"].dataType.simpleString() == "int"
    assert result.schema["price"].dataType.simpleString() == "double"


def test_cast_types_missing_params_raises():
    with pytest.raises(ValueError, match="cast_types requires"):
        CastTypesTransformer({}).transform(None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# drop_duplicates
# ---------------------------------------------------------------------------

def test_drop_duplicates_all_columns(spark):
    df = spark.createDataFrame([
        Row(id=1, val="a"),
        Row(id=1, val="a"),
        Row(id=2, val="b"),
    ])
    result = DropDuplicatesTransformer({}).transform(df)
    assert result.count() == 2


def test_drop_duplicates_subset(spark):
    df = spark.createDataFrame([
        Row(id=1, val="a"),
        Row(id=1, val="b"),  # same id, different val — should be dropped by subset
        Row(id=2, val="c"),
    ])
    result = DropDuplicatesTransformer({"subset": ["id"]}).transform(df)
    assert result.count() == 2


def test_drop_duplicates_no_duplicates_unchanged(spark):
    df = spark.createDataFrame([Row(id=1), Row(id=2), Row(id=3)])
    result = DropDuplicatesTransformer({}).transform(df)
    assert result.count() == 3


# ---------------------------------------------------------------------------
# filter_nulls
# ---------------------------------------------------------------------------

def test_filter_nulls_specific_columns(spark):
    df = spark.createDataFrame([
        Row(id=1, name="Alice"),
        Row(id=None, name="Bob"),   # id is null — dropped
        Row(id=3, name=None),       # name is null — kept (not in columns list)
    ])
    result = FilterNullsTransformer({"columns": ["id"]}).transform(df)
    assert result.count() == 2


def test_filter_nulls_all_columns(spark):
    df = spark.createDataFrame([
        Row(id=1, name="Alice"),
        Row(id=None, name="Bob"),
        Row(id=3, name=None),
    ])
    result = FilterNullsTransformer({}).transform(df)
    assert result.count() == 1


def test_filter_nulls_no_nulls_unchanged(spark):
    df = spark.createDataFrame([Row(id=1, name="Alice"), Row(id=2, name="Bob")])
    result = FilterNullsTransformer({"columns": ["id", "name"]}).transform(df)
    assert result.count() == 2


# ---------------------------------------------------------------------------
# factory
# ---------------------------------------------------------------------------

def test_factory_resolves_all_builtins():
    for name in ("cast_types", "drop_duplicates", "filter_nulls"):
        t = TransformerFactory.get(name, {})
        assert t is not None


def test_factory_unknown_raises():
    with pytest.raises(ValueError, match="Unknown transformer"):
        TransformerFactory.get("nonexistent", {})
