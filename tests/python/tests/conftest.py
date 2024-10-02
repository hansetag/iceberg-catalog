import dataclasses
import json
import os
import urllib
import uuid

import pyiceberg.catalog
import pyiceberg.catalog.rest
import pyiceberg.typedef
import pytest
import requests

# ---- Core
MANAGEMENT_URL = os.environ.get("ICEBERG_REST_TEST_MANAGEMENT_URL")
CATALOG_URL = os.environ.get("ICEBERG_REST_TEST_CATALOG_URL")
# ---- S3
S3_ACCESS_KEY = os.environ.get("ICEBERG_REST_TEST_S3_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("ICEBERG_REST_TEST_S3_SECRET_KEY")
S3_BUCKET = os.environ.get("ICEBERG_REST_TEST_S3_BUCKET")
S3_ENDPOINT = os.environ.get("ICEBERG_REST_TEST_S3_ENDPOINT")
S3_REGION = os.environ.get("ICEBERG_REST_TEST_S3_REGION", None)
S3_PATH_STYLE_ACCESS = os.environ.get("ICEBERG_REST_TEST_S3_PATH_STYLE_ACCESS")
S3_STS_MODE = os.environ.get("ICEBERG_REST_TEST_S3_STS_MODE", "both")
# ---- ADLS
AZURE_CLIENT_ID = os.environ.get("ICEBERG_REST_TEST_AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.environ.get("ICEBERG_REST_TEST_AZURE_CLIENT_SECRET")
AZURE_STORAGE_ACCOUNT_NAME = os.environ.get(
    "ICEBERG_REST_TEST_AZURE_STORAGE_ACCOUNT_NAME"
)
AZURE_STORAGE_FILESYSTEM = os.environ.get("ICEBERG_REST_TEST_AZURE_STORAGE_FILESYSTEM")
AZURE_TENANT_ID = os.environ.get("ICEBERG_REST_TEST_AZURE_TENANT_ID")
# ---- GCS
GCS_CREDENTIAL = os.environ.get("ICEBERG_REST_TEST_GCS_CREDENTIAL")
GCS_BUCKET = os.environ.get("ICEBERG_REST_TEST_GCS_BUCKET")
# ---- OAUTH
OPENID_PROVIDER_URI = os.environ.get("ICEBERG_REST_TEST_OPENID_PROVIDER_URI")
OPENID_CLIENT_ID = os.environ.get("ICEBERG_REST_TEST_OPENID_CLIENT_ID")
OPENID_CLIENT_SECRET = os.environ.get("ICEBERG_REST_TEST_OPENID_CLIENT_SECRET")

# ---- TRINO
TRINO_URI = os.environ.get("ICEBERG_REST_TEST_TRINO_URI")
# ---- SPARK
SPARK_ICEBERG_VERSION = os.environ.get(
    "ICEBERG_REST_TEST_SPARK_ICEBERG_VERSION", "1.5.2"
)
# ---- STARROCKS
STARROCKS_URI = os.environ.get("ICEBERG_REST_TEST_STARROCKS_URI")

STORAGE_CONFIGS = []

if S3_ACCESS_KEY is not None:
    if S3_STS_MODE == "both":
        STORAGE_CONFIGS.append({"type": "s3", "sts-enabled": True})
        STORAGE_CONFIGS.append({"type": "s3", "sts-enabled": False})
    elif S3_STS_MODE == "enabled":
        STORAGE_CONFIGS.append({"type": "s3", "sts-enabled": True})
    elif S3_STS_MODE == "disabled":
        STORAGE_CONFIGS.append({"type": "s3", "sts-enabled": False})
    else:
        raise ValueError(
            f"Invalid ICEBERG_REST_TEST_S3_STS_MODE: {S3_STS_MODE}. "
            "must be one of 'both', 'enabled', 'disabled'"
        )

if AZURE_CLIENT_ID is not None:
    STORAGE_CONFIGS.append({"type": "azure"})

if GCS_CREDENTIAL is not None:
    STORAGE_CONFIGS.append({"type": "gcs"})


def string_to_bool(s: str) -> bool:
    return s.lower() in ["true", "1"]


@pytest.fixture(scope="session", params=STORAGE_CONFIGS)
def storage_config(request) -> dict:
    if request.param["type"] == "s3":
        if S3_BUCKET is None:
            pytest.skip("ICEBERG_REST_TEST_S3_BUCKET is not set")

        if S3_PATH_STYLE_ACCESS is not None:
            path_style_access = string_to_bool(S3_PATH_STYLE_ACCESS)
        else:
            path_style_access = None

        if S3_REGION is None:
            pytest.skip("ICEBERG_REST_TEST_S3_REGION is not set")

        return {
            "storage-profile": {
                "type": "s3",
                "bucket": S3_BUCKET,
                "region": S3_REGION,
                "path-style-access": path_style_access,
                "endpoint": S3_ENDPOINT,
                "flavor": "minio",
                "sts-enabled": request.param["sts-enabled"],
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": S3_ACCESS_KEY,
                "aws-secret-access-key": S3_SECRET_KEY,
            },
        }
    elif request.param["type"] == "azure":
        if AZURE_STORAGE_ACCOUNT_NAME is None:
            pytest.skip("ICEBERG_REST_TEST_AZURE_STORAGE_ACCOUNT_NAME is not set")
        if AZURE_STORAGE_FILESYSTEM is None:
            pytest.skip("ICEBERG_REST_TEST_AZURE_STORAGE_FILESYSTEM is not set")

        return {
            "storage-profile": {
                "type": "azdls",
                "account-name": AZURE_STORAGE_ACCOUNT_NAME,
                "filesystem": AZURE_STORAGE_FILESYSTEM,
            },
            "storage-credential": {
                "type": "az",
                "credential-type": "client-credentials",
                "client-id": AZURE_CLIENT_ID,
                "client-secret": AZURE_CLIENT_SECRET,
                "tenant-id": AZURE_TENANT_ID,
            },
        }
    elif request.param["type"] == "gcs":
        if GCS_BUCKET is None:
            pytest.skip("ICEBERG_REST_TEST_GCS_BUCKET is not set")

        return {
            "storage-profile": {
                "type": "gcs",
                "bucket": GCS_BUCKET,
            },
            "storage-credential": {
                "type": "gcs",
                "credential-type": "service-account-key",
                "key": json.loads(GCS_CREDENTIAL),
            },
        }
    else:
        raise ValueError(f"Unknown storage type: {request.param['type']}")


@pytest.fixture(scope="session")
def io_fsspec(storage_config: dict):
    import fsspec

    if storage_config["storage-profile"]["type"] == "s3":
        fs = fsspec.filesystem(
            "s3",
            anon=False,
            key=storage_config["storage-credential"]["aws-access-key-id"],
            secret=storage_config["storage-credential"]["aws-secret-access-key"],
            client_kwargs={
                "region_name": storage_config["storage-profile"]["region"],
                "endpoint_url": storage_config["storage-profile"]["endpoint"],
                "use_ssl": False,
            },
        )

    return fs


@dataclasses.dataclass
class Server:
    catalog_url: str
    management_url: str
    access_token: str

    def create_warehouse(
        self, name: str, project_id: uuid.UUID, storage_config: dict
    ) -> uuid.UUID:
        """Create a warehouse in this server"""
        create_payload = {
            "project-id": str(project_id),
            "warehouse-name": name,
            **storage_config,
        }

        warehouse_url = self.warehouse_url
        response = requests.post(
            warehouse_url,
            json=create_payload,
            headers={"Authorization": f"Bearer {self.access_token}"},
        )
        if not response.ok:
            raise ValueError(
                f"Failed to create warehouse ({response.status_code}): {response.text}"
            )

        warehouse_id = response.json()["warehouse-id"]
        return uuid.UUID(warehouse_id)

    @property
    def warehouse_url(self) -> str:
        return urllib.parse.urljoin(self.management_url, "v1/warehouse")


@dataclasses.dataclass
class Warehouse:
    server: Server
    project_id: uuid.UUID
    warehouse_id: uuid.UUID
    warehouse_name: str
    access_token: str

    @property
    def pyiceberg_catalog(self) -> pyiceberg.catalog.rest.RestCatalog:
        return pyiceberg.catalog.rest.RestCatalog(
            name="my_catalog_name",
            uri=self.server.catalog_url,
            warehouse=f"{self.project_id}/{self.warehouse_name}",
            token=self.access_token,
        )

    @property
    def normalized_catalog_name(self) -> str:
        return f"catalog_{self.warehouse_name.replace('-', '_')}"


@dataclasses.dataclass
class Namespace:
    name: pyiceberg.typedef.Identifier
    warehouse: Warehouse

    @property
    def pyiceberg_catalog(self) -> pyiceberg.catalog.rest.RestCatalog:
        return self.warehouse.pyiceberg_catalog

    @property
    def spark_name(self) -> str:
        return "`" + ".".join(self.name) + "`"


@pytest.fixture(scope="session")
def access_token() -> str:
    if OPENID_PROVIDER_URI is None:
        pytest.skip("OAUTH_PROVIDER_URI is not set")

    token_endpoint = requests.get(
        OPENID_PROVIDER_URI.strip("/") + "/.well-known/openid-configuration"
    ).json()["token_endpoint"]
    response = requests.post(
        token_endpoint,
        data={"grant_type": "client_credentials"},
        auth=(OPENID_CLIENT_ID, OPENID_CLIENT_SECRET),
    )
    response.raise_for_status()
    return response.json()["access_token"]


@pytest.fixture(scope="session")
def server(access_token) -> Server:
    if MANAGEMENT_URL is None:
        pytest.skip("ICEBERG_REST_TEST_MANAGEMENT_URL is not set")
    if CATALOG_URL is None:
        pytest.skip("ICEBERG_REST_TEST_CATALOG_URL is not set")

    return Server(
        catalog_url=CATALOG_URL.rstrip("/") + "/",
        management_url=MANAGEMENT_URL.rstrip("/") + "/",
        access_token=access_token,
    )


@pytest.fixture(scope="session")
def warehouse(server: Server, storage_config) -> Warehouse:
    project_id = uuid.uuid4()
    test_id = uuid.uuid4()
    warehouse_name = f"warehouse-{test_id}"
    warehouse_id = server.create_warehouse(
        warehouse_name, project_id=project_id, storage_config=storage_config
    )
    return Warehouse(
        access_token=server.access_token,
        server=server,
        project_id=project_id,
        warehouse_id=warehouse_id,
        warehouse_name=warehouse_name,
    )


@pytest.fixture(scope="function")
def namespace(warehouse: Warehouse):
    catalog = warehouse.pyiceberg_catalog
    namespace = (f"namespace-{uuid.uuid4()}",)
    catalog.create_namespace(namespace)
    return Namespace(name=namespace, warehouse=warehouse)


@pytest.fixture(scope="session")
def spark(warehouse: Warehouse):
    """Spark with a pre-configured Iceberg catalog"""
    try:
        import findspark

        findspark.init()
    except ImportError:
        pytest.skip("findspark not installed")

    import pyspark
    import pyspark.sql

    pyspark_version = pyspark.__version__
    # Strip patch version
    pyspark_version = ".".join(pyspark_version.split(".")[:2])

    print(f"SPARK_ICEBERG_VERSION: {SPARK_ICEBERG_VERSION}")
    spark_jars_packages = (
        f"org.apache.iceberg:iceberg-spark-runtime-{pyspark_version}_2.12:{SPARK_ICEBERG_VERSION},"
        f"org.apache.iceberg:iceberg-aws-bundle:{SPARK_ICEBERG_VERSION},"
        f"org.apache.iceberg:iceberg-azure-bundle:{SPARK_ICEBERG_VERSION},"
        f"org.apache.iceberg:iceberg-gcp-bundle:{SPARK_ICEBERG_VERSION}"
    )
    # random 5 char string
    catalog_name = warehouse.normalized_catalog_name
    configuration = {
        "spark.jars.packages": spark_jars_packages,
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": catalog_name,
        f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog_name}.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
        f"spark.sql.catalog.{catalog_name}.uri": warehouse.server.catalog_url,
        f"spark.sql.catalog.{catalog_name}.credential": f"{OPENID_CLIENT_ID}:{OPENID_CLIENT_SECRET}",
        f"spark.sql.catalog.{catalog_name}.warehouse": f"{warehouse.project_id}/{warehouse.warehouse_name}",
        f"spark.sql.catalog.{catalog_name}.oauth2-server-uri": f"{OPENID_PROVIDER_URI.rstrip('/')}/protocol/openid-connect/token",
    }

    spark_conf = pyspark.SparkConf().setMaster("local[*]")

    for k, v in configuration.items():
        spark_conf = spark_conf.set(k, v)

    spark = pyspark.sql.SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sql(f"USE {catalog_name}")
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def trino(warehouse: Warehouse, storage_config):
    if TRINO_URI is None:
        pytest.skip("ICEBERG_REST_TEST_TRINO_URI is not set")

    from trino.dbapi import connect

    conn = connect(host=TRINO_URI, user="trino")

    cur = conn.cursor()
    if storage_config["storage-profile"]["type"] == "s3":
        extra_config = f"""
            ,
            "s3.region" = 'dummy',
            "s3.path-style-access" = 'true',
            "s3.endpoint" = '{S3_ENDPOINT}',
            "fs.native-s3.enabled" = 'true'
        """
    elif storage_config["storage-profile"]["type"] == "azdls":
        extra_config = """
            ,
            "fs.native-azure.enabled" = 'true'
        """
    else:
        raise ValueError(
            f"Unknown storage type: {storage_config['storage-profile']['type']}"
        )

    cur.execute(
        f"""
        CREATE CATALOG {warehouse.normalized_catalog_name} USING iceberg
        WITH (
            "iceberg.catalog.type" = 'rest',
            "iceberg.rest-catalog.uri" = '{warehouse.server.catalog_url}',
            "iceberg.rest-catalog.warehouse" = '{warehouse.project_id}/{warehouse.warehouse_name}',
            "iceberg.rest-catalog.security" = 'OAUTH2',
            "iceberg.rest-catalog.oauth2.token" = '{warehouse.access_token}',
            "iceberg.rest-catalog.vended-credentials-enabled" = 'true'
            {extra_config}
        )
    """
    )

    conn = connect(
        host=TRINO_URI,
        user="trino",
        catalog=warehouse.normalized_catalog_name,
    )

    yield conn


@pytest.fixture(scope="session")
def starrocks(warehouse: Warehouse, storage_config):
    if STARROCKS_URI is None:
        pytest.skip("ICEBERG_REST_TEST_STARROCKS_URI is not set")

    from sqlalchemy import create_engine

    engine = create_engine(STARROCKS_URI)
    connection = engine.connect()
    connection.execute("DROP CATALOG IF EXISTS rest_catalog")

    storage_type = storage_config["storage-profile"]["type"]

    if storage_type == "s3":
        # Use the following when https://github.com/StarRocks/starrocks/issues/50585#issue-2501162084
        # is fixed:
        # connection.execute(
        #     f"""
        #     CREATE EXTERNAL CATALOG rest_catalog
        #     PROPERTIES
        #     (
        #         "type" = "iceberg",
        #         "iceberg.catalog.type" = "rest",
        #         "iceberg.catalog.uri" = "{warehouse.server.catalog_url}",
        #         "iceberg.catalog.warehouse" = "{warehouse.project_id}/{warehouse.warehouse_name}",
        #         "header.x-iceberg-access-delegation" = "vended-credentials",
        #         "iceberg.catalog.oauth2-server-uri" = "{OPENID_PROVIDER_URI.rstrip('/')}/protocol/openid-connect/token",
        #         "iceberg.catalog.credential" = "{OPENID_CLIENT_ID}:{OPENID_CLIENT_SECRET}"
        #     )
        #     """
        # )
        connection.execute(
            f"""
            CREATE EXTERNAL CATALOG rest_catalog
            PROPERTIES
            (
                "type" = "iceberg",
                "iceberg.catalog.type" = "rest",
                "iceberg.catalog.uri" = "{warehouse.server.catalog_url}",
                "iceberg.catalog.warehouse" = "{warehouse.project_id}/{warehouse.warehouse_name}",
                "iceberg.catalog.oauth2-server-uri" = "{OPENID_PROVIDER_URI.rstrip('/')}/protocol/openid-connect/token",
                "iceberg.catalog.credential" = "{OPENID_CLIENT_ID}:{OPENID_CLIENT_SECRET}",
                "aws.s3.region" = "local",
                "aws.s3.enable_path_style_access" = "true",
                "aws.s3.endpoint" = "{S3_ENDPOINT}",
                "aws.s3.access_key" = "{S3_ACCESS_KEY}",
                "aws.s3.secret_key" = "{S3_SECRET_KEY}"
            )
            """
        )
    else:
        raise ValueError(f"Unknown storage type for starrocks: {storage_type}")
    connection.execute("SET CATALOG rest_catalog")

    yield connection
