import conftest
import pandas as pd
import pytest


def test_create_namespace(starrocks, warehouse: conftest.Warehouse):
    starrocks.execute("CREATE DATABASE test_create_namespace_sr")
    assert (
        "test_create_namespace_sr",
    ) in warehouse.pyiceberg_catalog.list_namespaces()


def test_list_namespaces(starrocks, warehouse: conftest.Warehouse):
    starrocks.execute("CREATE DATABASE test_list_namespaces_starrocks_1")
    starrocks.execute("CREATE DATABASE test_list_namespaces_starrocks_2")
    r = starrocks.execute("SHOW DATABASES").fetchall()
    assert ("test_list_namespaces_starrocks_1",) in r
    assert ("test_list_namespaces_starrocks_2",) in r


def test_namespace_create_if_not_exists(starrocks, warehouse: conftest.Warehouse):
    starrocks.execute(
        "CREATE DATABASE IF NOT EXISTS test_namespace_create_if_not_exists_starrocks"
    )
    starrocks.execute(
        "CREATE DATABASE IF NOT EXISTS test_namespace_create_if_not_exists_starrocks"
    )
    assert (
        "test_namespace_create_if_not_exists_starrocks",
    ) in warehouse.pyiceberg_catalog.list_namespaces()


def test_drop_namespace(starrocks, warehouse: conftest.Warehouse):
    starrocks.execute("CREATE DATABASE test_drop_namespace_starrocks")
    assert (
        "test_drop_namespace_starrocks",
    ) in warehouse.pyiceberg_catalog.list_namespaces()
    starrocks.execute("DROP DATABASE test_drop_namespace_starrocks")
    assert (
        "test_drop_namespace_starrocks",
    ) not in warehouse.pyiceberg_catalog.list_namespaces()


def test_create_table(starrocks, warehouse: conftest.Warehouse):
    starrocks.execute("CREATE DATABASE test_create_table_starrocks")
    starrocks.execute(
        "CREATE TABLE test_create_table_starrocks.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR)"
    )
    loaded_table = warehouse.pyiceberg_catalog.load_table(
        ("test_create_table_starrocks", "my_table")
    )
    assert len(loaded_table.schema().fields) == 3


def test_drop_table(starrocks, warehouse: conftest.Warehouse):
    starrocks.execute("CREATE DATABASE test_drop_table_starrocks")
    starrocks.execute(
        "CREATE TABLE test_drop_table_starrocks.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR)"
    )

    assert warehouse.pyiceberg_catalog.load_table(
        ("test_drop_table_starrocks", "my_table")
    )

    starrocks.execute("DROP TABLE test_drop_table_starrocks.my_table")
    with pytest.raises(Exception) as e:
        warehouse.pyiceberg_catalog.load_table(
            ("test_drop_table_starrocks", "my_table")
        )
        assert "NoSuchTableError" in str(e)


def test_create_table_with_data(starrocks, warehouse: conftest.Warehouse):
    starrocks.execute("CREATE DATABASE test_create_table_with_data_starrocks")
    starrocks.execute(
        "CREATE TABLE test_create_table_with_data_starrocks.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR)"
    )
    starrocks.execute(
        "INSERT INTO test_create_table_with_data_starrocks.my_table VALUES (1, 1.0, 'a'), (2, 2.0, 'b')"
    )
    rows = starrocks.execute(
        "SELECT * FROM test_create_table_with_data_starrocks.my_table",
    )
    assert rows.fetchall() == [(1, 1.0, "a"), (2, 2.0, "b")]


def test_write_read_data(starrocks, warehouse: conftest.Warehouse):
    starrocks.execute("CREATE DATABASE test_write_read_data_starrocks")
    starrocks.execute(
        "CREATE TABLE test_write_read_data_starrocks.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR)"
    )
    starrocks.execute(
        "INSERT INTO test_write_read_data_starrocks.my_table VALUES (1, 1.0, 'a'), (2, 2.0, 'b')"
    )
    rows = starrocks.execute(
        "SELECT * FROM test_write_read_data_starrocks.my_table",
    )
    assert rows.fetchall() == [(1, 1.0, "a"), (2, 2.0, "b")]


def test_partition(starrocks, warehouse: conftest.Warehouse):
    starrocks.execute("CREATE DATABASE test_partition_starrocks")
    # Partition columns must be at the end for starrocks
    starrocks.execute(
        "CREATE TABLE test_partition_starrocks.my_table (my_floats DOUBLE, strings VARCHAR, my_ints INT) PARTITION BY (my_ints)"
    )
    starrocks.execute(
        "INSERT INTO test_partition_starrocks.my_table VALUES (1.0, 'a', 1), (2.0, 'b', 2)"
    )
    rows = starrocks.execute(
        "SELECT * FROM test_partition_starrocks.my_table",
    )
    assert set(rows.fetchall()) == {(1.0, "a", 1), (2.0, "b", 2)}


def test_alter_table_add_column(starrocks, warehouse: conftest.Warehouse):
    starrocks.execute("CREATE DATABASE test_alter_partition_starrocks")
    starrocks.execute(
        "CREATE TABLE test_alter_partition_starrocks.my_table (my_floats DOUBLE, strings VARCHAR, my_ints INT) PARTITION BY (my_ints)"
    )
    starrocks.execute(
        "INSERT INTO test_alter_partition_starrocks.my_table VALUES (1.0, 'a', 1), (2.0, 'b', 2)"
    )
    starrocks.execute(
        "ALTER TABLE test_alter_partition_starrocks.my_table ADD COLUMN my_new_column INT"
    )
    starrocks.execute(
        "INSERT INTO test_alter_partition_starrocks.my_table VALUES (3.0, 'c', 3, 13)"
    )
    rows = starrocks.execute(
        "SELECT * FROM test_alter_partition_starrocks.my_table",
    )
    assert set(rows.fetchall()) == {
        (1.0, "a", 1, None),
        (2.0, "b", 2, None),
        (3.0, "c", 3, 13),
    }


def test_empty_table(starrocks):
    starrocks.execute("CREATE DATABASE test_empty_table_starrocks")
    starrocks.execute(
        "CREATE TABLE test_empty_table_starrocks.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR)"
    )
    rows = starrocks.execute(
        "SELECT * FROM test_empty_table_starrocks.my_table",
    )
    assert rows.fetchall() == []
