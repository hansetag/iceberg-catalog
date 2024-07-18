// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Integration tests for Iceberg Datafusion with Hive Metastore.

use anyhow::Context;
use clap::{Parser, Subcommand};
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::CatalogProvider;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{DataFrame, SessionConfig};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_datafusion::IcebergCatalogProvider;
use iceberg_ext::iceberg::{Catalog, ErrorKind, NamespaceIdent, Result, TableCreation};
use iceberg_ext::{iceberg, iceberg_datafusion};
use reqwest::Url;
use std::collections::HashMap;
use std::sync::Arc;

fn get_catalog(rest_catalog_uri: Url, props: &HashMap<String, String>) -> RestCatalog {
    let config = RestCatalogConfig::builder()
        .uri(rest_catalog_uri)
        .warehouse("demo".to_string())
        .props(props.clone())
        .build();

    RestCatalog::new(config)
}

async fn set_test_namespace(catalog: &RestCatalog, namespace: &NamespaceIdent) -> Result<()> {
    let properties = HashMap::new();

    catalog.create_namespace(namespace, properties).await?;

    Ok(())
}

fn set_table_creation(name: impl ToString) -> Result<TableCreation> {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let creation = TableCreation::builder()
        .name(name.to_string())
        .properties(HashMap::new())
        .schema(schema)
        .build();

    Ok(creation)
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    rest_catalog_uri: Url,
    #[clap(default_value = "default")]
    namespace: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Migrate the database
    Migrate {},
    /// Run the server - The database must be migrated before running the server
    Serve {},
    /// Check the health of the server
    Healthcheck {},
    /// Print the version of the server
    Version {},
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Cli {
        rest_catalog_uri,
        namespace,
    } = Cli::parse();
    let namespace = NamespaceIdent::new(namespace);
    let props = HashMap::new();

    let catalog = get_catalog(rest_catalog_uri, &props);
    match set_test_namespace(&catalog, &namespace).await {
        Ok(_) => println!("Namespace created"),
        Err(e) => match e.kind() {
            ErrorKind::DataInvalid => {
                if e.message().contains("Namespace already exists") {
                    println!("Namespace already exists");
                }
            }
            _ => return Err(anyhow::Error::new(e)),
        },
    };

    let creation = set_table_creation("my_table")?;
    let tab = catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(catalog);
    let catalogp = Arc::new(IcebergCatalogProvider::try_new(client).await?);
    let mut cfg = SessionConfig::new().set_bool("datafusion.catalog.information_schema", true);

    let ctx = SessionContext::new_with_config(cfg);

    ctx.register_catalog("rest", catalogp);
    let provider = ctx.catalog("rest").unwrap();

    ctx.sql("CREATE table if not exists rest.default.my_table (id int, name string);")
        .await?;
    eprintln!("{}", provider.schema_names().join(", "));
    dbg!(ctx.sql("SHOW tables").await?.collect().await?);
    ctx.sql("INSERT INTO rest.default.my_table VALUES (1, 'a');")
        .await?
        .write_table("rest.default.my_table", DataFrameWriteOptions::default())
        .await?;

    let df = ctx
        .table("rest.default.my_table")
        .await
        .with_context(|| "get table")?;
    dbg!(df.collect().await.with_context(|| "collect table")?);

    Ok(())
}

// #[tokio::test]
// async fn test_provider_list_table_names() -> Result<()> {
//     let fixture = get_test_fixture().await;
//     let namespace = NamespaceIdent::new("test_provider_list_table_names".to_string());
//     set_test_namespace(&fixture.hms_catalog, &namespace).await?;
//
//     let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
//     fixture
//         .hms_catalog
//         .create_table(&namespace, creation)
//         .await?;
//
//     let client = Arc::new(fixture.get_catalog());
//     let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);
//
//     let ctx = SessionContext::new();
//     ctx.register_catalog("hive", catalog);
//
//     let provider = ctx.catalog("hive").unwrap();
//     let schema = provider.schema("test_provider_list_table_names").unwrap();
//
//     let expected = vec!["my_table"];
//     let result = schema.table_names();
//
//     assert_eq!(result, expected);
//
//     Ok(())
// }
//
// #[tokio::test]
// async fn test_provider_list_schema_names() -> Result<()> {
//     let fixture = get_test_fixture().await;
//     let namespace = NamespaceIdent::new("test_provider_list_schema_names".to_string());
//     set_test_namespace(&fixture.hms_catalog, &namespace).await?;
//
//     set_table_creation("test_provider_list_schema_names", "my_table")?;
//     let client = Arc::new(fixture.get_catalog());
//     let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);
//
//     let ctx = SessionContext::new();
//     ctx.register_catalog("hive", catalog);
//
//     let provider = ctx.catalog("hive").unwrap();
//
//     let expected = ["default", "test_provider_list_schema_names"];
//     let result = provider.schema_names();
//
//     assert!(expected
//         .iter()
//         .all(|item| result.contains(&item.to_string())));
//     Ok(())
// }
