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
