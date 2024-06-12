use anyhow::Context;
use async_nats::ServerAddr;
use clap::{Parser, Subcommand};
use iceberg_catalog::service::contract_verification::ContractVerifiers;
use iceberg_catalog::service::event_publisher::{
    CloudEventSink, CloudEventsPublisher, NatsPublisher,
};
use iceberg_catalog::{
    implementations::{
        postgres::{Catalog, CatalogState, SecretsState, SecretsStore},
        AllowAllAuthState, AllowAllAuthZHandler,
    },
    service::router::{new_full_router, serve as service_serve},
    CONFIG,
};
use std::sync::Arc;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Migrate the database
    Migrate {},
    /// Run the server - The database must be migrated before running the server
    Serve {},
    /// Check the health of the server
    Healthcheck {},
}

async fn serve(bind_addr: std::net::SocketAddr) -> Result<(), anyhow::Error> {
    let read_pool = iceberg_catalog::implementations::postgres::get_reader_pool().await?;
    let write_pool = iceberg_catalog::implementations::postgres::get_writer_pool().await?;

    let catalog_state = CatalogState {
        read_pool: read_pool.clone(),
        write_pool: write_pool.clone(),
    };
    let secrets_state = SecretsState {
        read_pool,
        write_pool,
    };

    let mut cloud_event_sinks = vec![];

    if let Some(nat_addr) = &CONFIG.nats_address {
        tracing::info!("Running with nats publisher, connecting to: {nat_addr}");
        let nats_publisher = NatsPublisher {
            client: async_nats::connect(
                ServerAddr::from_url(nat_addr.clone())
                    .context("Converting nats URL to ServerAddr failed.")?,
            )
            .await
            .context("Connecting to nats server failed.")?,
            topic: CONFIG
                .nats_topic
                .clone()
                .ok_or(anyhow::anyhow!("Missing nats topic."))?,
        };
        cloud_event_sinks.push(Arc::new(nats_publisher) as Arc<dyn CloudEventSink + Sync + Send>);
    } else {
        tracing::info!("Running without publisher.");
    };

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let router = new_full_router::<
        Catalog,
        Catalog,
        AllowAllAuthZHandler,
        AllowAllAuthZHandler,
        SecretsStore,
    >(
        AllowAllAuthState,
        catalog_state,
        secrets_state,
        CloudEventsPublisher {
            sinks: cloud_event_sinks,
        },
        ContractVerifiers::new(vec![]),
    );

    service_serve(listener, router).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(true)
        .with_file(true)
        .with_line_number(true)
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    match cli.command {
        Some(Commands::Migrate {}) => {
            println!("Migrating database...");
            let write_pool = iceberg_catalog::implementations::postgres::get_writer_pool().await?;

            // This embeds database migrations in the application binary so we can ensure the database
            // is migrated correctly on startup
            iceberg_catalog::implementations::postgres::migrate(&write_pool).await?;
            println!("Database migration complete.");
        }
        Some(Commands::Serve {}) => {
            println!("Starting server on 0.0.0.0:8080...");
            let bind_addr = std::net::SocketAddr::from(([0, 0, 0, 0], 8080));
            serve(bind_addr).await?;
        }
        Some(Commands::Healthcheck {}) => {
            println!("Checking health...");
            let client = reqwest::Client::new();
            let response = client.get("http://localhost:8080/health").send().await?;
            let status = response.status();
            // Fail with an error if the server is not healthy
            if !status.is_success() {
                eprintln!("Server is not healthy: {}", status);
                std::process::exit(1);
            } else {
                println!("Server is healthy.");
            }
        }
        None => {
            // Error out if no subcommand is provided.
            eprintln!("No subcommand provided. Use --help for more information.");
        }
    }

    Ok(())
}
