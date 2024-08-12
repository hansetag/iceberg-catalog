# Contributing to Iceberg-Catalog
All commits to main should go through a PR. CI checks should pass before merging the PR.
Before merge commits are squashed. PR titles should follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

## Quickstart

```shell
# start postgres & vault
docker run -d --name postgres-15 -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15
docker run -d -p 8200:8200 --cap-add=IPC_LOCK -e 'VAULT_DEV_ROOT_TOKEN_ID=myroot' -e 'VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200' hashicorp/vault
# set envs
echo 'export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres' > .env
echo 'export ICEBERG_REST__PG_ENCRYPTION_KEY="abc"' >> .env
echo 'export ICEBERG_REST__PG_DATABASE_URL_READ="postgresql://postgres:postgres@localhost/postgres"' >> .env
echo 'export ICEBERG_REST__PG_DATABASE_URL_WRITE="postgresql://postgres:postgres@localhost/postgres"' >> .env
echo 'export ICEBERG_REST__KV2__URL="http://localhost:8200"' >> .env
echo 'export ICEBERG_REST__KV2__USER="test"' >> .env
echo 'export ICEBERG_REST__KV2__PASSWORD="test"' >> .env
echo 'export ICEBERG_REST__KV2__SECRET_MOUNT="secret"' >> .env
source .env

# setup vault
./tests/vault-setup.sh http://localhost:8200

# migrate db
cd crates/iceberg-catalog
sqlx database create && sqlx migrate run
cd ../..

# run tests
cargo test --all-features --all-targets

# run clippy
cargo clippy --all-features --all-targets
```


## Developing with docker compose

The following shell snippet will start a full development environment including the catalog plus its dependencies and a jupyter server with spark. The iceberg-catalog and its migrations will be built from source. This can be useful for development and testing.

```sh
$ cd examples
$ docker-compose -f docker-compose.yaml -f docker-compose-latest.yaml up -d --build
```

You may then head to `localhost:8888` and try out one of the notebooks.

## Working with SQLx

This crate uses sqlx. For development and compilation a Postgres Database is required. You can use Docker to launch
one.:

```sh
docker run -d --name postgres-15 -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15
```
The `crates/iceberg-catalog` folder contains a `.env.sample` File.
Copy this file to `.env` and add your database credentials if they differ.

Run:

```sh
sqlx database create
sqlx migrate run
```

## KV2 / Vault

This catalog supports KV2 as backend for secrets. This means running all tests via:

```sh
cargo test --all-features --all-targets
```

will fail if no vault is running. You can use the following commands to set the vault server up for testing:

```sh
$ docker run -d -p 8200:8200 --cap-add=IPC_LOCK -e 'VAULT_DEV_ROOT_TOKEN_ID=myroot' -e 'VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200' hashicorp/vault
$ ./scripts/vault-setup.sh localhost:8200
```

## Test cloud storage profiles

Currently, we're not aware of a good way of testing cloud storage integration against local deployments. That means, in order to test against AWS s3 & Azure Datalake Storage Gen 2, you need to set the following environment variables for more information take a look at the storage guide ([STORAGE.md](STORAGE.md)), a sample `.env` could look like this:

```sh
# TEST_AZURE=<some-value> controls a proc macro which either includes or excludes the azure tests
# if you compiled without TEST_AZURE, you'll have to change a file or do a cargo clean before rerunning tests. The same applies for the TEST_AWS and TEST_MINIO env vars.
export TEST_AZURE=1
export AZURE_TENANT_ID=<your tenant id>
export AZURE_CLIENT_ID=<your entra id app registration client id>
export AZURE_CLIENT_SECRET=<your entra id app registration client secret>
export AZURE_STORAGE_ACCOUNT_NAME=<your azure storage account name>
export AZURE_STORAGE_FILESYSTEM=<your azure adls filesystem name>

export TEST_AWS=1
export AWS_S3_BUCKET=<your aws s3 bucket>
export AWS_S3_REGION=<your aws s3 region>
# replace with actual values
export AWS_S3_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_S3_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_S3_STS_ROLE_ARN=arn:aws:iam::123456789012:role/role-name

# the values below should work with the default minio in our docker-compose
export TEST_MINIO=1
export ICEBERG_REST_TEST_S3_BUCKET=tests
export ICEBERG_REST_TEST_S3_REGION=local
export ICEBERG_REST_TEST_S3_ACCESS_KEY=minio-root-user
export ICEBERG_REST_TEST_S3_SECRET_KEY=minio-root-password
export ICEBERG_REST_TEST_S3_ENDPOINT=http://localhost:9000
```

You may then run a test via:

```sh
source .example.env-from-above
cargo test service::storage::s3::test::aws::test_can_validate
```

## Running integration test

Please check the [Integration Test Docs](tests/README.md).
