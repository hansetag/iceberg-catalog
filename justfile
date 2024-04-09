set shell := ["bash", "-c"]

update-openapi:
    # Download from https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml and put into openapi folder
    curl -o openapi/rest-catalog-open-api.yaml https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml
    # For rust-server generation only:
    # Fix until https://github.com/OpenAPITools/openapi-generator/issues/7802 is resolved:
    # Parse the donwloaded yaml. Then set the for the existing object components.schemas.Namespace properties.length.type to integer
    # yq e '.components.schemas.Namespace.properties.length.type = "integer"' -i openapi/rest-catalog-open-api.yaml
    # Replace 5XX with 500 (gnu-sed)
    # gsed -i 's/5XX/500/g' openapi/rest-catalog-open-api.yaml

# Build handy packages to fetch structs from
build-openapi-client:
    podman run --rm \
        -v ${PWD}:/local openapitools/openapi-generator-cli:v7.4.0 generate \
        -i /local/openapi/rest-catalog-open-api.yaml \
        -g rust \
        -o /local/iceberg-rest-openapi \
        --server-variables=basePath= \
        -p packageName="iceberg-rest-openapi"

# Build handy packages to fetch structs from
build-openapi-axum:
    podman run --rm \
        -v ${PWD}:/local openapitools/openapi-generator-cli:v7.4.0 generate \
        -i /local/openapi/rest-catalog-open-api.yaml \
        -g rust-axum \
        -o /local/iceberg-rest-openapi-axum \
        --server-variables=basePath= \
        -p packageName="iceberg-rest-openapi-axum"
