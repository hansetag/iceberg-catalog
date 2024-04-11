use config::Config;

#[derive(Debug, Clone, serde::Deserialize, PartialEq)]
/// Configuration of the SAGA Module
pub struct DynAppConfig {
    pub bind_address: std::net::SocketAddr,
}

impl DynAppConfig {}

lazy_static::lazy_static! {
    #[derive(Debug)]
    /// Configurtion of the SAGA Module
    pub static ref CONFIG: DynAppConfig = {
        let config: DynAppConfig = Config::builder()
        .set_default("bind_address", "0.0.0.0:8080").expect("valid port")
        .add_source(
            config::Environment::with_prefix("ICEBERG_REST_SERVER")
            .try_parsing(true)
            .separator("__"))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();

        config
    };
}

#[cfg(test)]
mod test {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_default() {
        assert!(CONFIG.bind_address.to_string().starts_with(""))
    }
}
