# Current State
We are pre-alpha. As of now, this crate provides server stubs for the implementation of an Iceberg Rest Catalog.

This repo will become available under an open-source license. We are currently in the process of deciding which one. Stay tuned!


# Why we don't use `rust-server` generation
Currently there are multiple issues with Server generation that would require a lot of manual correction. Among others:
* No support for OneOf & AllOf (https://github.com/OpenAPITools/openapi-generator/issues/17210)
* No support for inline Enums (https://github.com/OpenAPITools/openapi-generator/blob/650e119f2201914234ea1575f9db00769adaa99c/modules/openapi-generator/src/main/resources/rust-server/models.mustache#L255)
* "5XX" codes in OpenAPI are generated in server stubs but are nonexistant
* The `iceberg` rust crate provides better structs with correct serializations that provide a number of convenient methods and builders.

