# Current State
WIP!

We are very much still in development. Please check the table below for an overview of implemented features.

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status][actions-badge]][actions-url]

[actions-badge]: https://github.com/hansetag/iceberg-rest-server/workflows/CI/badge.svg
[actions-url]: https://github.com/hansetag/iceberg-rest-server/actions?query=workflow%3ACI+branch%3Amain

# Features

This crate is still in development. Please find following an overview of currently supported features.
Please also check the Issues if you are missing something.

### Supported Operations - Iceberg-Rest

| Operation |    Status    | Description                                         |
|-----------|:------------:|-----------------------------------------------------|
| Namespace |   ![done]    | All operations implemented                          |
| Table     | ![semi-done] | Not all requirements / updates supported yet        |
| Views     |   ![open]    | Remove unused files and log entries                 |
| Metrics   |   ![open]    | Endpoint is available but doesn't store the metrics |

### Storage Profile Support

| Storage              |    Status    | Comment                                                          |
|----------------------|:------------:|------------------------------------------------------------------|
| S3 - AWS             | ![semi-done] | No vended-credentials - only remote-signing, assume role missing |
| S3 - Custom          |   ![done]    | Vended-Credentials not possible (AWS STS is missing)             |
| Azure Blob           |   ![open]    |                                                                  |
| Azure ADLS Gen2      |   ![open]    |                                                                  |
| Microsoft OneLake    |   ![open]    |                                                                  |
| Google Cloud Storage |   ![open]    |                                                                  |


### Supported Operations - Management API

| Operation          | Status  | Description                                        |
|--------------------|:-------:|----------------------------------------------------|
| Warehouse - Create | ![done] | Create a new Warehouse in a Project                |
| Warehouse - Update | ![open] | Update a Warehouse, i.e. its Storage               |
| Warehouse - Delete | ![open] | Delete a Warehouse                                 |
| AuthZ              | ![open] | Manage access to warehouses, namespaces and tables |
| More to come!      | ![open] |                                                    |

### Auth(N/Z) Handlers

| Operation       | Status  | Description                       |
|-----------------|:-------:|-----------------------------------|
| AllowAll        | ![done] | No AuthZ in place                 |
| OIDC (AuthN)    | ![open] | Secure access to tables via OIDC  |
| OpenFGA (AuthZ) | ![open] | Internal Authorization management |

### Supported Backends

| Backend                       | Status  | Comment |
|-------------------------------|:-------:|---------|
| Catalog: Postgres             | ![done] |         |
| Secrets: Postgres             | ![done] |         |
| Secrets: HashiCorp-Vault-Like | ![open] |         |


# Usage
For a working example please check the [`DEVELOPER_GUIDE.md`](./DEVELOPER_GUIDE.md).


[open]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueNeutral.svg
[semi-done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChangesGrey.svg
[done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg