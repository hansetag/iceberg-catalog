import conftest
import pandas as pd
import pytest


def test_create_namespace(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE my_namespace")
    assert ("my_namespace",) in warehouse.pyiceberg_catalog.list_namespaces()


def test_list_namespaces(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE my_namespace_1")
    spark.sql("CREATE NAMESPACE my_namespace_2")
    pdf = spark.sql("SHOW NAMESPACES").toPandas()
    assert len(pdf) == 2
    assert "my_namespace_1" in pdf["namespace"].values
    assert "my_namespace_2" in pdf["namespace"].values


def test_namespace_create_if_not_exists(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE my_namespace")
    try:
        spark.sql("CREATE NAMESPACE my_namespace")
    except Exception as e:
        assert "SCHEMA_ALREADY_EXISTS" in str(e)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS my_namespace")


def test_drop_namespace(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE my_namespace")
    assert ("my_namespace",) in warehouse.pyiceberg_catalog.list_namespaces()
    spark.sql("DROP NAMESPACE my_namespace")
    assert ("my_namespace",) not in warehouse.pyiceberg_catalog.list_namespaces()


def test_create_table(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE my_namespace")
    spark.sql(
        "CREATE TABLE my_namespace.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    loaded_table = warehouse.pyiceberg_catalog.load_table(("my_namespace", "my_table"))
    assert len(loaded_table.schema().fields) == 3


def test_drop_table(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE my_namespace")
    spark.sql(
        "CREATE TABLE my_namespace.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    assert warehouse.pyiceberg_catalog.load_table(("my_namespace", "my_table"))
    spark.sql("DROP TABLE my_namespace.my_table")
    with pytest.raises(Exception) as e:
        warehouse.pyiceberg_catalog.load_table(("my_namespace", "my_table"))
        assert "NoSuchTableError" in str(e)


def test_query_empty_table(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE my_namespace")
    spark.sql(
        "CREATE TABLE my_namespace.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    pdf = spark.sql("SELECT * FROM my_namespace.my_table").toPandas()
    assert pdf.empty
    assert len(pdf.columns) == 3


def test_table_properties(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE my_namespace")
    spark.sql(
        "CREATE TABLE my_namespace.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(
        "ALTER TABLE my_namespace.my_table SET TBLPROPERTIES ('key1'='value1', 'key2'='value2')"
    )
    pdf = (
        spark.sql("SHOW TBLPROPERTIES my_namespace.my_table")
        .toPandas()
        .set_index("key")
    )
    assert pdf.loc["key1"]["value"] == "value1"
    assert pdf.loc["key2"]["value"] == "value2"


def test_write_read_table(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE my_namespace")
    spark.sql(
        "CREATE TABLE my_namespace.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(
        "INSERT INTO my_namespace.my_table VALUES (1, 1.2, 'foo'), (2, 2.2, 'bar')"
    )
    pdf = spark.sql("SELECT * FROM my_namespace.my_table").toPandas()
    assert len(pdf) == 2
    assert pdf["my_ints"].tolist() == [1, 2]
    assert pdf["my_floats"].tolist() == [1.2, 2.2]
    assert pdf["strings"].tolist() == ["foo", "bar"]


def test_list_tables(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE my_namespace")
    spark.sql(
        "CREATE TABLE my_namespace.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    pdf = spark.sql("SHOW TABLES IN my_namespace").toPandas()
    assert len(pdf) == 1
    assert pdf["tableName"].values[0] == "my_table"
