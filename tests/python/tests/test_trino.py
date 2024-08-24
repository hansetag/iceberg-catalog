import conftest
import pandas as pd
import pytest


def test_create_namespace(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_create_namespace_trino")
    assert (
        "test_create_namespace_trino",
    ) in warehouse.pyiceberg_catalog.list_namespaces()


def test_list_namespaces(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_list_namespaces_trino_1")
    cur.execute("CREATE SCHEMA test_list_namespaces_trino_2")
    r = cur.execute("SHOW SCHEMAS").fetchall()
    assert ["test_list_namespaces_trino_1"] in r
    assert ["test_list_namespaces_trino_2"] in r


def test_namespace_create_if_not_exists(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS test_namespace_create_if_not_exists_trino")
    cur.execute("CREATE SCHEMA IF NOT EXISTS test_namespace_create_if_not_exists_trino")
    assert (
        "test_namespace_create_if_not_exists_trino",
    ) in warehouse.pyiceberg_catalog.list_namespaces()


def test_drop_namespace(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_drop_namespace_trino")
    assert (
        "test_drop_namespace_trino",
    ) in warehouse.pyiceberg_catalog.list_namespaces()
    cur.execute("DROP SCHEMA test_drop_namespace_trino")
    assert (
        "test_drop_namespace_trino",
    ) not in warehouse.pyiceberg_catalog.list_namespaces()


# def test_create_table(trino, warehouse: conftest.Warehouse):
#     cur = trino.cursor()
#     cur.execute("CREATE SCHEMA test_create_table_trino")
#     cur.execute(
#         "CREATE TABLE test_create_table_trino.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
#     )
#     loaded_table = warehouse.pyiceberg_catalog.load_table(
#         ("test_create_table_trino", "my_table")
#     )
#     assert len(loaded_table.schema().fields) == 3


# def test_create_table_with_data(trino, warehouse: conftest.Warehouse):
#     cur = trino.cursor()
#     cur.execute("CREATE SCHEMA test_create_table_with_data_trino")
#     cur.execute(
#         "CREATE TABLE test_create_table_with_data_trino.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH ('format'='PARQUET')"
#     )
#     cur.execute(
#         "INSERT INTO test_create_table_with_data_trino.my_table VALUES (1, 1.0, 'a'), (2, 2.0, 'b')"
#     )
