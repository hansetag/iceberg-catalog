
# Modifications after `rust-axum` generation
* Delete all FromStr / ToStr / FromHeaderValues / ToHeaderValues that don't compile. We don't use them anyway.

# Why we don't use `rust-server` generation
Currently there are multiple issues with Server generation that would require a lot of manual correction. Among others:
* No support for OneOf & AllOf (https://github.com/OpenAPITools/openapi-generator/issues/17210)
* No support for inline Enums (https://github.com/OpenAPITools/openapi-generator/blob/650e119f2201914234ea1575f9db00769adaa99c/modules/openapi-generator/src/main/resources/rust-server/models.mustache#L255)
* "5XX" codes in OpenAPI are generated in server stubs but nonexistant
