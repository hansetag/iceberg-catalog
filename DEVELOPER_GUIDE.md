# Contributing to Iceberg-Catalog
All commits to main should go through a PR. CI checks should pass before merging the PR.
Before merge commits are squashed. PR titles should follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).


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

## Running integration test

Please check the [Integration Test Docs](tests/README.md).
