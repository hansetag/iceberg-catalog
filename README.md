# Iceberg Catalog - The TIP of the Iceberg
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status][actions-badge]][actions-url]

[actions-badge]: https://github.com/hansetag/iceberg-rest-server/workflows/CI/badge.svg?branch=main
[actions-url]: https://github.com/hansetag/iceberg-rest-server/actions?query=workflow%3ACI+branch%3Amain

This is TIP: A Rust-native implementation of the [Apache Iceberg](https://iceberg.apache.org/) REST Catalog specification.

# Scope and Features
The Iceberg REST Protocol has become the standard for catalogs in open Lakehouses. It natively enables multi-table commits, server-side deconflicting and much more.

We have started this implementation because we were missing customizability, support for on-premise deployments and features that are important for us in other Iceberg Catalogs. Please find following some of our focuses with this implementation:

* **Customizable**: If you already manage Access to Tables in your company somewhere else or need the catalog to stream change events to a different system, you can do so with by implementing just a few methods. Please find more details in the [Customization Guide](CUSTOMIZING.md).
* **Change Events**: Built-in support to emit change events (CloudEvents), which enables you to react to any change that happen to your tables. Changes can also be prohibited by external systems using our request / response handler. This is can be used to prohibit changes to tables that would validate Data Contracts.
* **Multi-Tenant capable**: A single deployment of our server can serve multiple projects - all with a single entrypoint. All Iceberg and Warehouse configurations are completly separated between Warehouses.
* **Written in Rust**: Single 18Mb all-in-one binary - no JVM or Python env required.
* **Storage Access Management**: Built-in S3-Signing that enables support for self-hosted as well as AWS S3 WITHOUT sharing S3 credentials with clients.
* **Well-Tested**: Integration-tested with `spark`, `trino` and `pyiceberg` (support for S3 with this catalog from pyiceberg 0.7.0)
* **High Available & Horizontally Scalable**: There is no local state - the catalog can be scaled horizontally and updated without downtimes.
* **Fine Grained Access (FGA) (Coming soon):** Simple Role-Based access control is not enough for many rapidly evolving Data & Analytics initiatives. We are leveraging OpenFGA based on googles [Zanzibar-Paper](https://research.google/pubs/zanzibar-googles-consistent-global-authorization-system/) to implement authorization. If your company already has a different system in place, you can integrate with it by implementing a handful of methods in the `AuthZHandler` trait.

Please find following an overview of currently supported features. Please also check the Issues if you are missing something.

# Quickstart

We have prepared a self-contained docker-compose file to demonstrate the usage of `spark` with our catalog:

```sh
git clone https://github.com/hansetag/iceberg-rest-server.git
cd iceberg-rest-server/examples
docker compose up
```

Then open your browser and head to `localhost:8888`.

# Status

### Supported Operations - Iceberg-Rest

| Operation | Status  | Description                                                              |
|-----------|:-------:|--------------------------------------------------------------------------|
| Namespace | ![done] | All operations implemented                                               |
| Table     | ![done] | All operations implemented - additional integration tests in development |
| Views     | ![open] | Remove unused files and log entries                                      |
| Metrics   | ![open] | Endpoint is available but doesn't store the metrics                      |

### Storage Profile Support

| Storage              |    Status    | Comment                                                          |
|----------------------|:------------:|------------------------------------------------------------------|
| S3 - AWS             | ![semi-done] | No vended-credentials - only remote-signing, assume role missing |
| S3 - Custom          |   ![done]    | Vended-Credentials not possible (AWS STS is missing)             |
| Azure Blob           |   ![open]    |                                                                  |
| Azure ADLS Gen2      |   ![open]    |                                                                  |
| Microsoft OneLake    |   ![open]    |                                                                  |
| Google Cloud Storage |   ![open]    |                                                                  |


### Supported Catalog Backends

| Backend  | Status  | Comment |
|----------|:-------:|---------|
| Postgres | ![done] |         |
| MongoDB  | ![open] |         |


### Supported Secret Stores
| Backend              | Status  | Comment |
|----------------------|:-------:|---------|
| Postgres             | ![done] |         |
| HashiCorp-Vault-Like | ![open] |         |

### Supported Event Stores
| Backend | Status  | Comment |
|---------|:-------:|---------|
| Nats    | ![done] |         |
| Kafka   | ![open] |         |

### Supported Operations - Management API

| Operation            | Status  | Description                                        |
|----------------------|:-------:|----------------------------------------------------|
| Warehouse Management | ![done] | Create / Update / Delete a Warehouse               |
| AuthZ                | ![open] | Manage access to warehouses, namespaces and tables |
| More to come!        | ![open] |                                                    |

### Auth(N/Z) Handlers

| Operation       | Status  | Description                                                                                                        |
|-----------------|:-------:|--------------------------------------------------------------------------------------------------------------------|
| OIDC (AuthN)    | ![open] | Secure access to tables via OIDC                                                                                   |
| Custom (AuthZ)  | ![done] | If you are willing to implement a single rust Trait, the `AuthZHandler` can be implement to connect to your system |
| OpenFGA (AuthZ) | ![open] | Internal Authorization management                                                                                  |

# Multiple Projects
The iceberg-rest server can host multiple independent warehouses that are again grouped by projects. The overall structure looks like this:

```
<project-1-uuid>/
├─ foo-warehouse
├─ bar-warehouse
<project-2-uuid>/
├─ foo-warehouse
├─ bas-warehouse
  
```

All warehouses use isolated namespaces and can be configured in client by specifying `warehouse` as `'<project-uuid>/<warehouse-name>'`. Warehouse Names inside Projects must be unique. We recommend using human readable names for warehouses.

If you do not need the hierarchy level of projects, set the `ICEBERG_REST__DEFAULT_PROJECT_ID` environment variable to the project you want to use. For single project deployments we recommend using the NULL UUID ("00000000-0000-0000-0000-000000000000") as project-id. Users then just specify `warehouse` as `<warehouse-name>` when connecting.

# Limitations
* Table Metadata is currently limited to `256Mb` for the `postgres` implementation. If you need more, you should probably vaccum your table ;)

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)


[open]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueNeutral.svg
[semi-done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChangesGrey.svg
[done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg
