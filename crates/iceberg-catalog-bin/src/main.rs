use anyhow::Error;
use clap::{Parser, Subcommand};
use iceberg_catalog::service::contract_verification::ContractVerifiers;
use iceberg_catalog::service::event_publisher::{
    CloudEventBackend, CloudEventsPublisher, CloudEventsPublisherBackgroundTask, Message,
    NatsBackend,
};
use iceberg_catalog::service::token_verification::Verifier;
use iceberg_catalog::{
    implementations::{
        postgres::{Catalog, CatalogState, SecretsState, SecretsStore},
        AllowAllAuthState, AllowAllAuthZHandler,
    },
    service::router::{new_full_router, serve as service_serve},
    CONFIG,
};
use reqwest::Url;
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
        let nats_publisher = build_nats_client(nat_addr).await?;
        cloud_event_sinks
            .push(Arc::new(nats_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
    } else {
        tracing::info!("Running without publisher.");
    };

    // TODO: what about this magic number
    let (tx, rx) = tokio::sync::mpsc::channel(1000);

    let x = CloudEventsPublisherBackgroundTask {
        source: rx,
        sinks: cloud_event_sinks,
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
        CloudEventsPublisher::new(tx.clone()),
        ContractVerifiers::new(vec![]),
        if let Some(uri) = CONFIG.openid_provider_uri.clone() {
            Some(Verifier::new(uri).await?)
        } else {
            None
        },
    );

    let publisher_handle = tokio::task::spawn(async move {
        match x.publish().await {
            Ok(_) => tracing::info!("Exiting publisher task"),
            Err(e) => tracing::error!("Publisher task failed: {e}"),
        };
    });

    service_serve(listener, router).await?;

    tracing::debug!("Sending shutdown signal to event publisher.");
    tx.send(Message::Shutdown).await?;
    publisher_handle.await?;

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

async fn build_nats_client(nat_addr: &Url) -> Result<NatsBackend, Error> {
    tracing::info!("Running with nats publisher, connecting to: {nat_addr}");
    let builder = async_nats::ConnectOptions::new();

    let builder = if let Some(file) = &CONFIG.nats_creds_file {
        builder.credentials_file(file).await?
    } else {
        builder
    };

    let builder = if let (Some(user), Some(pw)) = (&CONFIG.nats_user, &CONFIG.nats_password) {
        builder.user_and_password(user.clone(), pw.clone())
    } else {
        builder
    };

    let builder = if let Some(token) = &CONFIG.nats_token {
        builder.token(token.clone())
    } else {
        builder
    };

    let nats_publisher = NatsBackend {
        client: builder.connect(nat_addr.to_string()).await?,
        topic: CONFIG
            .nats_topic
            .clone()
            .ok_or(anyhow::anyhow!("Missing nats topic."))?,
    };
    Ok(nats_publisher)
}
