# Iceberg Rest Server
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status][actions-badge]][actions-url]

[actions-badge]: https://github.com/hansetag/iceberg-rest-server/workflows/CI/badge.svg
[actions-url]: https://github.com/hansetag/iceberg-rest-server/actions?query=workflow%3ACI+branch%3Amain

Native rust implementation of the [Apache Iceberg](https://iceberg.apache.org/) REST Catalog specification, based on [iceberg-rust](https://github.com/apache/iceberg-rust) and [axum](https://docs.rs/axum/latest/axum/).

For a minimal configuration, a postgres Database is currently the only external dependency required to run the server.
# Features

* Multi-Tenant capable: A single deployment of our server can serve multiple projects. Each project can in turn serve multiple `warehouses`.
* Single 18Mb all-in-one binary - no JVM or Python env required
* Built-in support to emit change events (CloudEvents), which enables you to react to any change that happen to your tables.
* Built-in S3-Signing that enables support for self-hosted S3 WITHOUT sharing S3 credentials with clients
* Production-grade API including server-side timeout handling, tracing and support for compression
* Tested with `spark`, `trino` and `pyiceberg`
* Extensible: Do you need to use your own enterprise solution to store secrets or integrate with your own special Authorization solution? If you are happy to implement a handful of rust methods yourself, you can easily integrate with those!

Please find following an overview of currently supported features.
Please also check the Issues if you are missing something.

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


### Supported Secret Stores
| Backend              | Status  | Comment |
|----------------------|:-------:|---------|
| Postgres             | ![done] |         |
| HashiCorp-Vault-Like | ![open] |         |

### Supported Operations - Management API

| Operation          | Status  | Description                                        |
|--------------------|:-------:|----------------------------------------------------|
| Warehouse - Create | ![done] | Create a new Warehouse in a Project                |
| Warehouse - Update | ![open] | Update a Warehouse, i.e. its Storage               |
| Warehouse - Delete | ![open] | Delete a Warehouse                                 |
| AuthZ              | ![open] | Manage access to warehouses, namespaces and tables |
| More to come!      | ![open] |                                                    |

### Auth(N/Z) Handlers

| Operation       | Status  | Description                                                                                                        |
|-----------------|:-------:|--------------------------------------------------------------------------------------------------------------------|
| OIDC (AuthN)    | ![open] | Secure access to tables via OIDC                                                                                   |
| Custom (AuthZ)  | ![done] | If you are willing to implement a single rust Trait, the `AuthZHandler` can be implement to connect to your system |
| OpenFGA (AuthZ) | ![open] | Internal Authorization management                                                                                  |

# Usage
For a working example please check the [`DEVELOPER_GUIDE.md`](./DEVELOPER_GUIDE.md).

# Limitations
* Table Metadata is currently limited to `256Mb` for the `postgres` implementation. If you need more, you should probably vaccum your table ;)

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)


[open]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueNeutral.svg
[semi-done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChangesGrey.svg
[done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg
