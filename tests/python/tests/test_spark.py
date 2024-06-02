import conftest
import pandas as pd
import pytest


def test_create_namespace(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_create_namespace_spark")
    assert (
        "test_create_namespace_spark",
    ) in warehouse.pyiceberg_catalog.list_namespaces()


def test_list_namespaces(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_list_namespaces_spark_1")
    spark.sql("CREATE NAMESPACE test_list_namespaces_spark_2")
    pdf = spark.sql("SHOW NAMESPACES").toPandas()
    assert "test_list_namespaces_spark_1" in pdf["namespace"].values
    assert "test_list_namespaces_spark_2" in pdf["namespace"].values


def test_namespace_create_if_not_exists(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_namespace_create_if_not_exists")
    try:
        spark.sql("CREATE NAMESPACE test_namespace_create_if_not_exists")
    except Exception as e:
        assert "SCHEMA_ALREADY_EXISTS" in str(e)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS test_namespace_create_if_not_exists")


def test_drop_namespace(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_drop_namespace")
    assert ("test_drop_namespace",) in warehouse.pyiceberg_catalog.list_namespaces()
    spark.sql("DROP NAMESPACE test_drop_namespace")
    assert ("test_drop_namespace",) not in warehouse.pyiceberg_catalog.list_namespaces()


def test_create_table(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_create_table")
    spark.sql(
        "CREATE TABLE test_create_table.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    loaded_table = warehouse.pyiceberg_catalog.load_table(
        ("test_create_table", "my_table")
    )
    assert len(loaded_table.schema().fields) == 3


def test_drop_table(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_drop_table")
    spark.sql(
        "CREATE TABLE test_drop_table.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    assert warehouse.pyiceberg_catalog.load_table(("test_drop_table", "my_table"))
    spark.sql("DROP TABLE test_drop_table.my_table")
    with pytest.raises(Exception) as e:
        warehouse.pyiceberg_catalog.load_table(("test_drop_table", "my_table"))
        assert "NoSuchTableError" in str(e)


def test_query_empty_table(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_query_empty_table")
    spark.sql(
        "CREATE TABLE test_query_empty_table.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    pdf = spark.sql("SELECT * FROM test_query_empty_table.my_table").toPandas()
    assert pdf.empty
    assert len(pdf.columns) == 3


def test_table_properties(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_table_properties")
    spark.sql(
        "CREATE TABLE test_table_properties.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(
        "ALTER TABLE test_table_properties.my_table SET TBLPROPERTIES ('key1'='value1', 'key2'='value2')"
    )
    pdf = (
        spark.sql("SHOW TBLPROPERTIES test_table_properties.my_table")
        .toPandas()
        .set_index("key")
    )
    assert pdf.loc["key1"]["value"] == "value1"
    assert pdf.loc["key2"]["value"] == "value2"


def test_write_read_table(spark):
    spark.sql("CREATE NAMESPACE test_write_read_table")
    spark.sql(
        "CREATE TABLE test_write_read_table.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(
        "INSERT INTO test_write_read_table.my_table VALUES (1, 1.2, 'foo'), (2, 2.2, 'bar')"
    )

    pdf = spark.sql("SELECT * FROM test_write_read_table.my_table").toPandas()
    assert len(pdf) == 2
    assert pdf["my_ints"].tolist() == [1, 2]
    assert pdf["my_floats"].tolist() == [1.2, 2.2]
    assert pdf["strings"].tolist() == ["foo", "bar"]


def test_list_tables(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_list_tables")
    spark.sql(
        "CREATE TABLE test_list_tables.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    pdf = spark.sql("SHOW TABLES IN test_list_tables").toPandas()
    assert len(pdf) == 1
    assert pdf["tableName"].values[0] == "my_table"


def test_single_partition_table(spark, namespace):
    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg PARTITIONED BY (my_ints)"
    )
    spark.sql(
        f"INSERT INTO {namespace.spark_name}.my_table VALUES (1, 1.2, 'foo'), (2, 2.2, 'bar')"
    )
    pdf = spark.sql(f"SELECT * FROM {namespace.spark_name}.my_table").toPandas()
    assert len(pdf) == 2
    assert pdf["my_ints"].tolist() == [1, 2]
    assert pdf["my_floats"].tolist() == [1.2, 2.2]
    assert pdf["strings"].tolist() == ["foo", "bar"]
    partitions = spark.sql(
        f"SELECT * FROM {namespace.spark_name}.my_table.partitions"
    ).toPandas()
    assert len(partitions) == 2


def test_partition_with_space_in_column_name(spark, namespace):
    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table (my_ints INT, `my floats` DOUBLE, strings STRING) USING iceberg PARTITIONED BY (`my floats`)"
    )
    spark.sql(
        f"INSERT INTO {namespace.spark_name}.my_table VALUES (1, 1.2, 'foo'), (2, 2.2, 'bar')"
    )


def test_partition_with_special_chars_in_name(spark, namespace):
    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table (my_ints INT, `m/y fl !? -_ä oats` DOUBLE, strings STRING) USING iceberg PARTITIONED BY (`m/y fl !? -_ä oats`)"
    )
    spark.sql(
        f"INSERT INTO {namespace.spark_name}.my_table VALUES (1, 1.2, 'foo'), (2, 2.2, 'bar')"
    )


def test_change_partitioning(spark, namespace):
    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg PARTITIONED BY (my_ints)"
    )
    spark.sql(
        f"INSERT INTO {namespace.spark_name}.my_table VALUES (1, 1.2, 'foo'), (2, 2.2, 'bar')"
    )
    spark.sql(
        f"ALTER TABLE {namespace.spark_name}.my_table DROP PARTITION FIELD my_ints"
    )

    spark.sql(f"INSERT INTO {namespace.spark_name}.my_table VALUES (3, 3.2, 'baz')")
    pdf = (
        spark.sql(f"SELECT * FROM {namespace.spark_name}.my_table")
        .toPandas()
        .sort_values(by="my_ints")
    )
    assert len(pdf) == 3
    assert pdf["my_ints"].tolist() == [1, 2, 3]
    assert pdf["my_floats"].tolist() == [1.2, 2.2, 3.2]
    assert pdf["strings"].tolist() == ["foo", "bar", "baz"]
    partitions = spark.sql(
        f"SELECT * FROM {namespace.spark_name}.my_table.partitions"
    ).toPandas()
    assert len(partitions) == 3


# ToDo: Fix {"error_id":"018fcac4-44a3-7333-b3c2-c0a4e0127339","message":"Field 'my_ints_bucket' not found in schema.","type":"FailedToBuildPartitionSpec","code":409,"stack":null}
def test_partition_bucket(spark, namespace):
    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg PARTITIONED BY (bucket(16, my_ints))"
    )
    spark.sql(
        f"INSERT INTO {namespace.spark_name}.my_table VALUES (1, 1.2, 'foo'), (2, 2.2, 'bar')"
    )
    pdf = spark.sql(f"SELECT * FROM {namespace.spark_name}.my_table").toPandas()
    assert len(pdf) == 2


def test_alter_schema(spark, namespace):
    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(f"INSERT INTO {namespace.spark_name}.my_table VALUES (1, 1.2, 'foo')")
    spark.sql(f"ALTER TABLE {namespace.spark_name}.my_table ADD COLUMN my_bool BOOLEAN")
    spark.sql(f"ALTER TABLE {namespace.spark_name}.my_table DROP COLUMN my_ints")

    spark.sql(f"INSERT INTO {namespace.spark_name}.my_table VALUES (1.2, 'bar', true)")
    pdf = spark.sql(f"SELECT * FROM {namespace.spark_name}.my_table").toPandas()
    assert len(pdf) == 2
