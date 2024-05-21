import conftest
import pyarrow as pa
import pytest


def test_create_namespace(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    namespace = ("my_namespace",)
    catalog.create_namespace(namespace)
    assert namespace in catalog.list_namespaces()


def test_list_namespaces(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    assert len(catalog.list_namespaces()) == 0
    catalog.create_namespace(("my_namespace_1",))
    catalog.create_namespace(("my_namespace_2"))
    namespaces = catalog.list_namespaces()
    assert len(namespaces) == 2
    assert ("my_namespace_1",) in namespaces
    assert ("my_namespace_2",) in namespaces


def test_namespace_properties(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    namespace = ("my_namespace",)
    properties = {"key-1": "value-1", "key2": "value2"}
    catalog.create_namespace(namespace, properties=properties)
    loaded_properties = catalog.load_namespace_properties(namespace)
    assert loaded_properties == properties


def test_drop_namespace(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    namespace = ("my_namespace",)
    catalog.create_namespace(namespace)
    assert namespace in catalog.list_namespaces()
    catalog.drop_namespace(namespace)
    assert namespace not in catalog.list_namespaces()


def test_create_table(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    namespace = ("my_namespace",)
    table_name = "my_table"
    schema = pa.schema(
        [
            pa.field("my_ints", pa.int64()),
            pa.field("my_floats", pa.float64()),
            pa.field("strings", pa.string()),
        ]
    )
    # Namespace is required:
    with pytest.raises(Exception) as e:
        catalog.create_table(table_name, schema=schema)
        assert "NamespaceNotFound" in str(e)

    catalog.create_namespace(namespace)
    catalog.create_table((*namespace, table_name), schema=schema)
    loaded_table = catalog.load_table((*namespace, table_name))
    assert len(loaded_table.schema().fields) == 3


def test_drop_table(namespace: conftest.Namespace):
    catalog = namespace.pyiceberg_catalog
    table_name = "my_table"
    schema = pa.schema(
        [
            pa.field("my_ints", pa.int64()),
            pa.field("my_floats", pa.float64()),
            pa.field("strings", pa.string()),
        ]
    )
    catalog.create_table((*namespace.name, table_name), schema=schema)
    assert catalog.load_table((*namespace.name, table_name))
    catalog.drop_table((*namespace.name, table_name))
    with pytest.raises(Exception) as e:
        catalog.load_table((*namespace.name, table_name))
        assert "NoSuchTableError" in str(e)


def test_table_properties(namespace: conftest.Namespace):
    catalog = namespace.pyiceberg_catalog
    table_name = "my_table"
    schema = pa.schema(
        [
            pa.field("my_ints", pa.int64()),
            pa.field("my_floats", pa.float64()),
            pa.field("strings", pa.string()),
        ]
    )
    properties = {"key-1": "value-1", "key2": "value2"}
    catalog.create_table(
        (*namespace.name, table_name), schema=schema, properties=properties
    )
    table = catalog.load_table((*namespace.name, table_name))
    assert table.properties == properties


def test_list_tables(namespace: conftest.Namespace):
    catalog = namespace.pyiceberg_catalog
    assert len(catalog.list_tables(namespace.name)) == 0
    table_name_1 = "my_table_1"
    table_name_2 = "my_table_2"
    schema = pa.schema(
        [
            pa.field("my_ints", pa.int64()),
            pa.field("my_floats", pa.float64()),
            pa.field("strings", pa.string()),
        ]
    )
    catalog.create_table((*namespace.name, table_name_1), schema=schema)
    catalog.create_table((*namespace.name, table_name_2), schema=schema)
    tables = catalog.list_tables(namespace.name)
    assert len(tables) == 2
    assert (*namespace.name, table_name_1) in tables
    assert (*namespace.name, table_name_2) in tables
