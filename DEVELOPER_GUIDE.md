## Working with SQLx
To work with SQLx, launch a postgres DB, for example with Docker:
```sh
docker run -d --name postgres-15 -p 5432:5432 -e POSTGRES_PASSWORD=pg-admin postgres:15
```
Each crate in the `crates` folder that uses SQLx contains a `.env.sample` File.
Copy this file to `.env` and add your database credentials.

Run:
```sh
sqlx database create
sqlx migrate run
```
## Running the binary

```sh
docker run -d --name postgres-15 -p 5432:5432 -e POSTGRES_PASSWORD=pg-admin postgres:15

export ICEBERG_REST__BASE_URI="http://localhost:8080/catalog/"
export ICEBERG_REST__PG_ENCRYPTION_KEY="abc"
export ICEBERG_REST__PG_DATABASE_URL_READ="postgresql://postgres:pg-admin@localhost/demo"
export ICEBERG_REST__PG_DATABASE_URL_WRITE="postgresql://postgres:pg-admin@localhost/demo"

cd src/crates/iceberg-rest-bin

cargo run migrate
# Optional - get some logs:
export RUST_LOG=info
cargo run serve
```

Now that the server is running, we need to create a new warehouse including its storage.
Lets assume we have an AWS S3-bucket, create a file called `create-warehouse-request.json`:
```json
{
    "warehouse_name": "test",
    "project_id": "00000000-0000-0000-0000-000000000000",
    "storage_profile": {
        "type": "s3",
        "bucket": "demo-catalog-iceberg",
        "key_prefix": "test_warehouse",
        "assume_role_arn": null,
        "endpoint": null,
        "region": "eu-central-1",
        "path_style_access": null
    },
    "storage_credential": {
        "type": "s3",
        "credential-type": "access-key",
        "aws_access_key_id": "<my-access-key>",
        "aws_secret_access_key": "<my-secret-access-key>"
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

