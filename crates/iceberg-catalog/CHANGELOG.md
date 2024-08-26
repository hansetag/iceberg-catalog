# Changelog

## [0.3.0](https://github.com/hansetag/iceberg-catalog/compare/v0.2.1...v0.3.0) (2024-08-26)


### ⚠ BREAKING CHANGES

* dots can no longer be used in namespace names ([#257](https://github.com/hansetag/iceberg-catalog/issues/257))

### Features

* Add support for custom Locations for Namespaces & Tables ([1d2ac6f](https://github.com/hansetag/iceberg-catalog/commit/1d2ac6f4b3910bf161c47d0224689b6e611d15ab))
* **aws:** sts credentials for s3 ([#236](https://github.com/hansetag/iceberg-catalog/issues/236)) ([dbf775b](https://github.com/hansetag/iceberg-catalog/commit/dbf775b6e226a8b8822f2e725ec317b4230aa0c4))
* **compression-codec:** Support setting and altering write.metadata.compression-codec ([#235](https://github.com/hansetag/iceberg-catalog/issues/235)) ([f4fb4cb](https://github.com/hansetag/iceberg-catalog/commit/f4fb4cbb4ce7f357db8d4d37dce8b92173402777))
* **storage:** add ability to narrow token permissions ([#249](https://github.com/hansetag/iceberg-catalog/issues/249)) ([ba9f046](https://github.com/hansetag/iceberg-catalog/commit/ba9f046cf48a380b7d0b6ce01a7f2045a9e47bea))
* **storage:** azdls ([#223](https://github.com/hansetag/iceberg-catalog/issues/223)) ([fd11428](https://github.com/hansetag/iceberg-catalog/commit/fd1142852555d239e8ea8dac2cb9d5db76457ab1))


### Bug Fixes

* dots can no longer be used in namespace names ([#257](https://github.com/hansetag/iceberg-catalog/issues/257)) ([8ac52e0](https://github.com/hansetag/iceberg-catalog/commit/8ac52e0e998c1417f3cb19655aebb4b39f054374))
* **kv2:** extend docs & fix mismatch between docs and expected env values ([#224](https://github.com/hansetag/iceberg-catalog/issues/224)) ([be3e3e6](https://github.com/hansetag/iceberg-catalog/commit/be3e3e60181acdb501303b7fb4215d79e65dd79e))

## [0.2.1](https://github.com/hansetag/iceberg-catalog/compare/v0.2.0...v0.2.1) (2024-07-29)


### Features

* **db:** Add Encryption Secret for postgres SecretStore to README & warn on startup ([#217](https://github.com/hansetag/iceberg-catalog/issues/217)) ([933409d](https://github.com/hansetag/iceberg-catalog/commit/933409da47aefb7b1fb9668386da35adab43477e))
* **secrets:** Secret Backend configuration is now case insensitive ([#215](https://github.com/hansetag/iceberg-catalog/issues/215)) ([99b19ab](https://github.com/hansetag/iceberg-catalog/commit/99b19ab3072fc4d9e2648a81cbca7b87b3b193b0))


### Bug Fixes

* **examples:** Fix `ICEBERG_REST__BASE_URI` ([33f213b](https://github.com/hansetag/iceberg-catalog/commit/33f213bf2592c958ac299a89ddae1a72e3446ed6))
* **s3signing:** Add S3 remote signing "content-md5" for pyiceberg compatability ([33f213b](https://github.com/hansetag/iceberg-catalog/commit/33f213bf2592c958ac299a89ddae1a72e3446ed6))


### Miscellaneous Chores

* release 0.2.1 ([587ea12](https://github.com/hansetag/iceberg-catalog/commit/587ea129780c21a3cd0fa8dd371b6901dede4c20))

## [0.2.0](https://github.com/hansetag/iceberg-catalog/compare/v0.1.0...v0.2.0) (2024-07-26)


### ⚠ BREAKING CHANGES

* Catalog base URL should not contain /catalog suffix ([#208](https://github.com/hansetag/iceberg-catalog/issues/208))
* **views:** split off tabular from table to prepare for views

### Features

* **health:** Service health checks ([#181](https://github.com/hansetag/iceberg-catalog/issues/181)) ([3bf4d4c](https://github.com/hansetag/iceberg-catalog/commit/3bf4d4c99e09b3ae90ea1b4a9aba5136300df514))
* **pagination:** add pagination for namespaces & tables & views ([#186](https://github.com/hansetag/iceberg-catalog/issues/186)) ([37b1dbd](https://github.com/hansetag/iceberg-catalog/commit/37b1dbd3fdd16c79e9f981d29c3842d7d7140564))
* **prometheus:** add prometheus axum metrics ([#185](https://github.com/hansetag/iceberg-catalog/issues/185)) ([d60d84a](https://github.com/hansetag/iceberg-catalog/commit/d60d84aebf26052a72e26ff6350d9636d4865009))
* **secrets:** add support for kv2 secret storage ([#192](https://github.com/hansetag/iceberg-catalog/issues/192)) ([a86b13c](https://github.com/hansetag/iceberg-catalog/commit/a86b13c5020cd52073608c74dacc86eff7e1bb60))
* **server:** make listenport configurable ([#183](https://github.com/hansetag/iceberg-catalog/issues/183)) ([9ffe0c2](https://github.com/hansetag/iceberg-catalog/commit/9ffe0c2e2c78b178bcb3900ed4d6a246e4eaeacb))
* **views:** authz interface for views & view-ident resolve ([#141](https://github.com/hansetag/iceberg-catalog/issues/141)) ([c5e1f99](https://github.com/hansetag/iceberg-catalog/commit/c5e1f99eba7244bdca9c37a42c3fe36f47c117a0))
* **views:** commit views ([#146](https://github.com/hansetag/iceberg-catalog/issues/146)) ([0f6310b](https://github.com/hansetag/iceberg-catalog/commit/0f6310b2486cc608af6844c35be7a45ebeb998cd))
* **views:** create + load view ([#142](https://github.com/hansetag/iceberg-catalog/issues/142)) ([328cf33](https://github.com/hansetag/iceberg-catalog/commit/328cf33cf268cdbb7df2f185ed228291e509d6ab))
* **views:** exists ([#149](https://github.com/hansetag/iceberg-catalog/issues/149)) ([fdb5013](https://github.com/hansetag/iceberg-catalog/commit/fdb501326f72734a7faafc685402ef7d12e1189c))
* **views:** list-views ([5917a5e](https://github.com/hansetag/iceberg-catalog/commit/5917a5e853e1a3c03f47cbad9152b74f9b88e9fa))
* **views:** rename views ([#148](https://github.com/hansetag/iceberg-catalog/issues/148)) ([4aaaa7d](https://github.com/hansetag/iceberg-catalog/commit/4aaaa7d6f727388c43a8ecc6f307a261b74abbef))
* **views:** split off tabular from table to prepare for views ([f62b329](https://github.com/hansetag/iceberg-catalog/commit/f62b3292e5fd9951dd20c6a48432e16c337db7a5))


### Bug Fixes

* Catalog base URL should not contain /catalog suffix ([#208](https://github.com/hansetag/iceberg-catalog/issues/208)) ([6aabaa9](https://github.com/hansetag/iceberg-catalog/commit/6aabaa97b1f8531830dd512c9a61c461c3f05b7f))
* **db:** add wait-for-db command ([#196](https://github.com/hansetag/iceberg-catalog/issues/196)) ([c1cd069](https://github.com/hansetag/iceberg-catalog/commit/c1cd069d773906a4c647dcc007c50b0aa6929c29))
* remove unused cfg-attributes ([#203](https://github.com/hansetag/iceberg-catalog/issues/203)) ([b6d17c4](https://github.com/hansetag/iceberg-catalog/commit/b6d17c4bbdef073962fd220faf4a632f4a64e541))
* **tables:** deny "write.metadata" & "write.data.path" table properties  ([#197](https://github.com/hansetag/iceberg-catalog/issues/197)) ([4b2191e](https://github.com/hansetag/iceberg-catalog/commit/4b2191e58439ce99a5420f411a121a2ba89a0698))

## [0.1.0](https://github.com/hansetag/iceberg-catalog/compare/v0.1.0-rc3...v0.1.0) (2024-06-17)


### Miscellaneous Chores

* 🚀 Release 0.1.0 ([a5def9a](https://github.com/hansetag/iceberg-catalog/commit/a5def9a527aa615779b60fe8fc5a18aaa47f33ee))

## [0.1.0-rc3](https://github.com/hansetag/iceberg-catalog/compare/v0.1.0-rc2...v0.1.0-rc3) (2024-06-17)


### Miscellaneous Chores

* 🚀 Release 0.1.0-rc3 ([9b0d219](https://github.com/hansetag/iceberg-catalog/commit/9b0d219e865dce85803fc93da7233e92d3e8b4b8))

## [0.1.0-rc2](https://github.com/hansetag/iceberg-catalog/compare/v0.1.0-rc1...v0.1.0-rc2) (2024-06-17)


### Bug Fixes

* add view router ([#116](https://github.com/hansetag/iceberg-catalog/issues/116)) ([0745cc8](https://github.com/hansetag/iceberg-catalog/commit/0745cc85e16974c05adc3b158f5cb04c9dd54ac4))


### Miscellaneous Chores

* 🚀 Release 0.1.0-rc2 ([9bc25ef](https://github.com/hansetag/iceberg-catalog/commit/9bc25ef2b44d6c29556a5d0913c076904b1cb010))

## [0.1.0-rc1](https://github.com/hansetag/iceberg-catalog/compare/v0.0.2-rc1...v0.1.0-rc1) (2024-06-16)


### Miscellaneous Chores

* 🚀 Release 0.1.0-rc1 ([ba6e5d5](https://github.com/hansetag/iceberg-catalog/commit/ba6e5d5c8a59cb1da5b61dd559c783998559debf))

## [0.0.2-rc1](https://github.com/hansetag/iceberg-catalog/compare/v0.0.1...v0.0.2-rc1) (2024-06-16)


### Miscellaneous Chores

* 🚀 Release 0.0.2-rc1 ([eb34b9c](https://github.com/hansetag/iceberg-catalog/commit/eb34b9cd613bb2d72d4a9b33b103d36c7649bd57))

## [0.0.1](https://github.com/hansetag/iceberg-catalog/compare/v0.0.0...v0.0.1) (2024-06-15)


### Miscellaneous Chores

* 🚀 Release 0.0.1 ([c52ddec](https://github.com/hansetag/iceberg-catalog/commit/c52ddec7520ec16ed0b6f70c5e3108a7d8a35665))
