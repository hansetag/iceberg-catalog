## Working with SQLx

This crate uses sqlx. For development and compilation a Postgres Database is required. You can use Docker to launch
one.:

```sh
docker run -d --name postgres-15 -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15
```

The server crate folder that uses SQLx contains a `.env.sample` File.
Copy this file to `.env` and add your database credentials if they differ.

Run:

```sh
sqlx database create
sqlx migrate run
```

## Running integration test

Please check the [Integration Test Docs](tests/README.md).

## Running the binary

```sh
docker run -d --name postgres-15 -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15

export ICEBERG_REST__BASE_URI="http://localhost:8080/catalog/"
export ICEBERG_REST__PG_ENCRYPTION_KEY="abc"
export ICEBERG_REST__PG_DATABASE_URL_READ="postgresql://postgres:postgres@localhost/demo"
export ICEBERG_REST__PG_DATABASE_URL_WRITE="postgresql://postgres:postgres@localhost/demo"

cd src/crates/iceberg-catalog-bin

cargo run migrate
# Optional - get some logs:
export RUST_LOG=info
cargo run serve
```

Now that the server is running, we need to create a new warehouse including its storage.
Lets assume we have an AWS S3-bucket, create a file called `create-warehouse-request.json`:

```json
{
  "warehouse-name": "test",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3",
    "bucket": "demo-catalog-iceberg",
    "key-prefix": "test_warehouse",
    "assume-role-arn": null,
    "endpoint": null,
    "region": "eu-central-1",
    "path-style-access": null
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "<my-access-key>",
    "aws-secret-access-key": "<my-secret-access-key>"
  }
}
```

We now create a new Warehouse by POSTing the request to the management API:

```sh
curl -X POST http://localhost:8080/management/v1/warehouse -H "Content-Type: application/json" -d @create-warehouse-request.json
```

That's it - we can now use the catalog:

```python
import pandas as pd
import pyspark

configuration = {
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.defaultCatalog": "demo",
    "spark.sql.catalog.demo": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.demo.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
    "spark.sql.catalog.demo.uri": "http://localhost:8080/catalog/",
    "spark.sql.catalog.demo.token": "dummy",
    "spark.sql.catalog.demo.warehouse": "00000000-0000-0000-0000-000000000000/test",
}
spark_conf = pyspark.SparkConf()
for k, v in configuration.items():
    spark_conf = spark_conf.set(k, v)

spark = pyspark.sql.SparkSession.builder.config(conf=spark_conf).getOrCreate()


spark.sql("USE demo")

spark.sql("CREATE NAMESPACE IF NOT EXISTS my_namespace")
print(f"\n\nCurrently the following namespace exist:")
print(spark.sql("SHOW NAMESPACES").toPandas())
print("\n\n")

sdf = spark.createDataFrame(
    pd.DataFrame(
        [[1, 1.2, "foo"], [2, 2.2, "bar"]], columns=["my_ints", "my_floats", "strings"]
    )
)

spark.sql("DROP TABLE IF EXISTS demo.my_namespace.my_table")
spark.sql(
    "CREATE TABLE demo.my_namespace.my_table (my_ints INT, my_floats DOUBLE, strings STRING) USING iceberg"
)
sdf.writeTo("demo.my_namespace.my_table").append()
spark.table("demo.my_namespace.my_table").show()
```
