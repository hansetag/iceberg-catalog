## Conduct

The Iceberg Rest Server project adheres to the [Rust Code of Conduct][coc]. This describes
the _minimum_ behavior expected from all contributors. Instances of violations of the
Code of Conduct can be reported by contacting the project team at
[moderation@hansetag.com](mailto:moderation@hansetag.com).

[coc]: https://github.com/rust-lang/rust/blob/master/CODE_OF_CONDUCT.md

## Working with SQLx
To work with SQLx, launch a postgres DB, for example with Docker:
```sh
docker run -d --name postgres-15 -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15
```
Each crate in the `crates` folder that uses SQLx contains a `.env.sample` File.
Copy this file to `.env` and modify your database credentials should they differ.

Run:
```sh
sqlx database create
sqlx migrate run
```
