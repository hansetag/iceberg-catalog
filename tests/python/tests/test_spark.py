import conftest
import pandas as pd
import pytest
import requests
import pyiceberg.io as io
import time


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
    spark.sql("CREATE NAMESPACE test_create_table_spark")
    spark.sql(
        "CREATE TABLE test_create_table_spark.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    loaded_table = warehouse.pyiceberg_catalog.load_table(
        ("test_create_table_spark", "my_table")
    )
    assert len(loaded_table.schema().fields) == 3


def test_create_table_with_data(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_create_table_pyspark")
    data = pd.DataFrame([[1, "a-string", 2.2]], columns=["id", "strings", "floats"])
    sdf = spark.createDataFrame(data)
    sdf.writeTo(f"test_create_table_pyspark.my_table").createOrReplace()


def test_replace_table(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_replace_table_pyspark")
    data = pd.DataFrame([[1, "a-string", 2.2]], columns=["id", "strings", "floats"])
    sdf = spark.createDataFrame(data)
    sdf.writeTo(f"test_replace_table_pyspark.my_table").createOrReplace()
    sdf.writeTo(f"test_replace_table_pyspark.my_table").createOrReplace()


def test_create_view(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_create_view")
    spark.sql(
        "CREATE TABLE test_create_view.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(
        "CREATE VIEW test_create_view.my_view AS SELECT my_ints, my_floats FROM test_create_view.my_table"
    )
    spark.sql("SELECT * from test_create_view.my_view")


def test_create_replace_view(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_create_replace_view_spark")
    spark.sql(
        "CREATE TABLE test_create_replace_view_spark.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(
        "CREATE VIEW test_create_replace_view_spark.my_view AS SELECT my_ints, my_floats FROM test_create_replace_view_spark.my_table"
    )

    df = spark.sql("SELECT * from test_create_replace_view_spark.my_view").toPandas()
    assert list(df.columns) == ["my_ints", "my_floats"]
    spark.sql(
        "CREATE OR REPLACE VIEW test_create_replace_view_spark.my_view AS SELECT my_floats, my_ints FROM test_create_replace_view_spark.my_table"
    )
    df = spark.sql("SELECT * from test_create_replace_view_spark.my_view").toPandas()
    assert list(df.columns) == ["my_floats", "my_ints"]


def test_rename_view(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_rename_view_spark")
    spark.sql(
        "CREATE TABLE test_rename_view_spark.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(
        "CREATE VIEW test_rename_view_spark.my_view AS SELECT my_ints, my_floats FROM test_rename_view_spark.my_table"
    )

    spark.sql("SELECT * from test_rename_view_spark.my_view")
    df = spark.sql("SHOW VIEWS IN test_rename_view_spark").toPandas()
    assert df.shape[0] == 1
    assert df["viewName"].values[0] == "my_view"

    spark.sql(
        "ALTER VIEW test_rename_view_spark.my_view RENAME TO test_rename_view_spark.my_view_renamed"
    )
    df = spark.sql("SHOW VIEWS IN test_rename_view_spark").toPandas()
    assert df.shape[0] == 1
    assert df["viewName"].values[0] == "my_view_renamed"


def test_create_drop_view(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_create_drop_view_spark")
    spark.sql(
        "CREATE TABLE test_create_drop_view_spark.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(
        "CREATE VIEW test_create_drop_view_spark.my_view AS SELECT my_ints, my_floats FROM test_create_drop_view_spark.my_table"
    )

    spark.sql("SELECT * from test_create_drop_view_spark.my_view")
    df = spark.sql("SHOW VIEWS IN test_create_drop_view_spark").toPandas()
    assert df.shape[0] == 1

    spark.sql("DROP VIEW test_create_drop_view_spark.my_view")
    df = spark.sql("SHOW VIEWS IN test_create_drop_view_spark").toPandas()
    assert df.shape[0] == 0


def test_view_exists(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_view_exists_spark")
    spark.sql(
        "CREATE TABLE test_view_exists_spark.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(
        "CREATE VIEW IF NOT EXISTS test_view_exists_spark.my_view AS SELECT my_ints, my_floats FROM test_view_exists_spark.my_table"
    )
    assert spark.sql("SHOW VIEWS IN test_view_exists_spark").toPandas().shape[0] == 1

    spark.sql(
        "CREATE VIEW IF NOT EXISTS test_view_exists_spark.my_view AS SELECT my_ints, my_floats FROM test_view_exists_spark.my_table"
    )
    assert spark.sql("SHOW VIEWS IN test_view_exists_spark").toPandas().shape[0] == 1


def test_merge_into(spark):
    spark.sql("CREATE NAMESPACE test_merge_into")
    spark.sql(
        "CREATE TABLE test_merge_into.my_table (id INT, strings STRING, floats DOUBLE) USING iceberg"
    )
    spark.sql(
        "INSERT INTO test_merge_into.my_table VALUES (1, 'a-string', 2.2), (2, 'b-string', 3.3)"
    )
    spark.sql(
        "MERGE INTO test_merge_into.my_table USING (SELECT 1 as id, 'c-string' as strings, 4.4 as floats) as new_data ON my_table.id = new_data.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    pdf = (
        spark.sql("SELECT * FROM test_merge_into.my_table").toPandas().sort_values("id")
    )
    assert len(pdf) == 2
    assert pdf["id"].tolist() == [1, 2]
    assert pdf["strings"].tolist() == ["c-string", "b-string"]
    assert pdf["floats"].tolist() == [4.4, 3.3]


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


@pytest.mark.xfail(reason="Spark purge tries to sign a request which we don't support")
def test_drop_table_purge_spark(spark, warehouse: conftest.Warehouse):
    spark.sql("CREATE NAMESPACE test_drop_table_purge_spark")
    spark.sql(
        "CREATE TABLE test_drop_table.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    assert warehouse.pyiceberg_catalog.load_table(("test_drop_table_purge_spark", "my_table"))
    spark.sql("DROP TABLE test_drop_table.my_table PURGE;")
    with pytest.raises(Exception) as e:
        warehouse.pyiceberg_catalog.load_table(("test_drop_table_purge_spark", "my_table"))
        assert "NoSuchTableError" in str(e)


def test_drop_table_purge_http(spark, warehouse: conftest.Warehouse, storage_config):
    namespace = "test_drop_table_purge_http"
    spark.sql(f"CREATE NAMESPACE {namespace}")
    dfs = []
    for n in range(5):
        data = pd.DataFrame([[1 + n, 'a-string', 2.2 + n]], columns=['id', 'strings', 'floats'])
        dfs.append(data)
        sdf = spark.createDataFrame(data)
        sdf.writeTo(f"{namespace}.my_table_{n}").create()

    for n, df in enumerate(dfs):
        table = warehouse.pyiceberg_catalog.load_table((namespace, f"my_table_{n}"))
        assert table
        assert table.scan().to_pandas().equals(df)

    table_0 = warehouse.pyiceberg_catalog.load_table((namespace, "my_table_0"))

    purge_uri = warehouse.server.catalog_url.strip("/") + "/" + "/".join(
        ["v1", str(warehouse.warehouse_id), "namespaces",
         namespace,
         "tables",
         "my_table_0?purgeRequested=True"])
    requests.delete(purge_uri,
                    headers={"Authorization": f"Bearer {warehouse.access_token}"}).raise_for_status()

    with pytest.raises(Exception) as e:
        warehouse.pyiceberg_catalog.load_table((namespace, "my_table_0"))
        assert "NoSuchTableError" in str(e)

    properties = table_0.properties
    if storage_config['storage-profile']['type'] == 's3':
        properties["s3.access-key-id"] = storage_config['storage-credential']['aws-access-key-id']
        properties["s3.secret-access-key"] = storage_config['storage-credential']['aws-secret-access-key']
        properties["s3.endpoint"] = storage_config['storage-profile']['endpoint']

    file_io = io._infer_file_io_from_scheme(table_0.location(), properties)

    # sleep to give time for the table to be gone
    time.sleep(5)

    inp = file_io.new_input(table_0.location())
    assert not inp.exists(), f"Table location {table_0.location()} still exists"

    tables = warehouse.pyiceberg_catalog.list_tables(namespace)
    assert len(tables) == 4
    for n, ((_, table), df) in enumerate(zip(sorted(tables), dfs[1:]), 1):
        assert table == f"my_table_{n}"
        table = warehouse.pyiceberg_catalog.load_table((namespace, table))
        assert table.scan().to_pandas().equals(df)


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


def test_alter_partitioning(spark, namespace):
    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(
        f"INSERT INTO {namespace.spark_name}.my_table VALUES (1, 1.2, 'foo'), (2, 2.2, 'bar')"
    )
    spark.sql(
        f"ALTER TABLE {namespace.spark_name}.my_table ADD PARTITION FIELD bucket(16, my_ints) as int_bucket"
    )
    spark.sql(
        f"INSERT INTO {namespace.spark_name}.my_table VALUES (3, 3.2, 'baz'), (4, 4.2, 'qux')"
    )
    pdf = spark.sql(f"SELECT * FROM {namespace.spark_name}.my_table").toPandas()
    assert len(pdf) == 4
    assert sorted(pdf["my_ints"].tolist()) == [1, 2, 3, 4]

    spark.sql(
        f"ALTER TABLE {namespace.spark_name}.my_table DROP PARTITION FIELD int_bucket"
    )
    spark.sql(
        f"ALTER TABLE {namespace.spark_name}.my_table ADD PARTITION FIELD truncate(4, strings) as string_bucket"
    )
    spark.sql(
        f"INSERT INTO {namespace.spark_name}.my_table VALUES (5, 5.2, 'foo'), (6, 6.2, 'bar')"
    )
    pdf = spark.sql(f"SELECT * FROM {namespace.spark_name}.my_table").toPandas()
    assert len(pdf) == 6
    assert sorted(pdf["strings"].tolist()) == ["bar", "bar", "baz", "foo", "foo", "qux"]


def test_tag_create(spark, namespace):
    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(f"INSERT INTO {namespace.spark_name}.my_table VALUES (1, 1.2, 'foo')")
    spark.sql(f"ALTER TABLE {namespace.spark_name}.my_table CREATE TAG first_insert")
    spark.sql(f"INSERT INTO {namespace.spark_name}.my_table VALUES (1, 1.2, 'foo')")
    pdf = spark.sql(
        f"SELECT * FROM {namespace.spark_name}.my_table VERSION AS OF 'first_insert'"
    ).toPandas()
    pdf2 = spark.sql(f"SELECT * FROM {namespace.spark_name}.my_table").toPandas()
    assert len(pdf) == 1
    assert len(pdf2) == 2


def test_tag_create_retain_365_days(spark, namespace):
    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(f"INSERT INTO {namespace.spark_name}.my_table VALUES (1, 1.2, 'foo')")
    spark.sql(
        f"ALTER TABLE {namespace.spark_name}.my_table CREATE TAG first_insert RETAIN 365 DAYS"
    )
    spark.sql(f"INSERT INTO {namespace.spark_name}.my_table VALUES (1, 1.2, 'foo')")
    pdf = spark.sql(
        f"SELECT * FROM {namespace.spark_name}.my_table VERSION AS OF 'first_insert'"
    ).toPandas()
    pdf2 = spark.sql(f"SELECT * FROM {namespace.spark_name}.my_table").toPandas()
    assert len(pdf) == 1
    assert len(pdf2) == 2


def test_branch_create(spark, namespace):
    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(f"INSERT INTO {namespace.spark_name}.my_table VALUES (1, 1.2, 'foo')")
    spark.sql(
        f"ALTER TABLE {namespace.spark_name}.my_table CREATE BRANCH test_branch RETAIN 7 DAYS"
    )
    pdf = spark.sql(f"SELECT * FROM {namespace.spark_name}.my_table.refs").toPandas()
    assert len(pdf) == 2


def test_branch_load_data(spark, namespace):
    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(f"INSERT INTO {namespace.spark_name}.my_table VALUES (1, 1.2, 'foo')")
    spark.sql(
        f"ALTER TABLE {namespace.spark_name}.my_table CREATE BRANCH test_branch RETAIN 7 DAYS"
    )
    spark.sql(
        f"INSERT INTO {namespace.spark_name}.my_table.branch_test_branch VALUES (2, 1.2, 'bar')"
    )
    pdf = spark.sql(f"SELECT * FROM {namespace.spark_name}.my_table").toPandas()
    pdf_b = spark.sql(
        f"SELECT * FROM {namespace.spark_name}.my_table.`branch_test_branch`"
    ).toPandas()
    assert len(pdf) == 1
    assert len(pdf_b) == 2


def test_table_maintenance_optimize(spark, namespace, warehouse: conftest.Warehouse):
    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
    )
    spark.sql(
        f"INSERT INTO {namespace.spark_name}.my_table VALUES (1, 1.2, 'foo'), (2, 2.2, 'bar')"
    )

    for i in range(5):
        spark.sql(
            f"INSERT INTO {namespace.spark_name}.my_table VALUES ({i}, 5.2, 'foo')"
        )

    number_files_begin = spark.sql(
        f"SELECT file_path FROM {namespace.spark_name}.my_table.files"
    ).toPandas()

    rewrite_result = spark.sql(
        f"CALL {warehouse.normalized_catalog_name}.system.rewrite_data_files(table=>'{namespace.spark_name}.my_table', options=>map('rewrite-all', 'true'))"
    ).toPandas()
    print(rewrite_result)

    number_files_end = spark.sql(
        f"SELECT file_path FROM {namespace.spark_name}.my_table.files"
    ).toPandas()

    assert len(number_files_begin) > 1
    assert len(number_files_end) == 1


def test_custom_location(spark, namespace, warehouse: conftest.Warehouse):
    # Create a table without a custom location to get the default location
    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table (my_ints INT) USING iceberg"
    )
    default_location = warehouse.pyiceberg_catalog.load_table(
        (*namespace.name, "my_table")
    ).location()

    # Replace element behind the last slash with "custom_location"
    custom_location = default_location.rsplit("/", 1)[0] + "/custom_location"

    # Create a table with a custom location
    spark.sql(
        f"CREATE TABLE {namespace.spark_name}.my_table_custom_location (my_ints INT) USING iceberg LOCATION '{custom_location}'"
    )
    # Write / read data
    spark.sql(
        f"INSERT INTO {namespace.spark_name}.my_table_custom_location VALUES (1), (2)"
    )
    pdf = spark.sql(
        f"SELECT * FROM {namespace.spark_name}.my_table_custom_location"
    ).toPandas()
    assert len(pdf) == 2

    # Check if the custom location is set correctly
    loaded_table = warehouse.pyiceberg_catalog.load_table(
        (*namespace.name, "my_table_custom_location")
    )
    assert loaded_table.location() == custom_location
    assert loaded_table.metadata_location.startswith(custom_location)
