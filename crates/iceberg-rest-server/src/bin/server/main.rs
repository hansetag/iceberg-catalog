mod config;
use config::CONFIG;
// use iceberg_rest_server::service::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let _addr = CONFIG.bind_address;

    // serve(addr).await?;

    Ok(())
}
