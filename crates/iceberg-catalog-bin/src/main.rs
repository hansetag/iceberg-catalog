use anyhow::{Context, Error};
use clap::{Parser, Subcommand};
use iceberg_catalog::api::management::v1::ManagementApiDoc;
use iceberg_catalog::implementations::postgres::{get_reader_pool, get_writer_pool, ReadWrite};
use iceberg_catalog::service::contract_verification::ContractVerifiers;
use iceberg_catalog::service::event_publisher::{
    CloudEventBackend, CloudEventsPublisher, CloudEventsPublisherBackgroundTask, Message,
    NatsBackend,
};
use iceberg_catalog::service::health::{
    HealthExt, HealthState, HealthStatus, ServiceHealthProvider,
};
use iceberg_catalog::service::token_verification::Verifier;
use iceberg_catalog::{
    api::router::{new_full_router, serve as service_serve},
    implementations::{
        postgres::{Catalog, CatalogState, SecretsState, SecretsStore},
        AllowAllAuthState, AllowAllAuthZHandler,
    },
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
    /// Migrate the database
    EnsureMigrate {},
    /// Run the server - The database must be migrated before running the server
    Serve {},
    /// Check the health of the server
    Healthcheck {
        #[clap(
            default_value = "false",
            short = 'a',
            help = "Check all services, implies -d and -s."
        )]
        check_all: bool,
        #[clap(
            default_value = "false",
            short = 'd',
            help = "Only test DB connection, requires postgres env values.",
            conflicts_with("check_all")
        )]
        check_db: bool,
        #[clap(
            default_value = "false",
            short = 's',
            help = "Check health endpoint.",
            conflicts_with("check_all")
        )]
        check_server: bool,
    },
    /// Print the version of the server
    Version {},
    /// Get the OpenAPI specification of the Management API as yaml
    ManagementOpenapi {},
}

async fn serve(bind_addr: std::net::SocketAddr) -> Result<(), anyhow::Error> {
    let read_pool = iceberg_catalog::implementations::postgres::get_reader_pool().await?;
    let write_pool = iceberg_catalog::implementations::postgres::get_writer_pool().await?;

    let catalog_state = CatalogState::from_pools(read_pool.clone(), write_pool.clone());
    let secrets_state = SecretsState::from_pools(read_pool, write_pool);
    let auth_state = AllowAllAuthState;

    let health_provider = ServiceHealthProvider::new(
        vec![
            ("catalog", Arc::new(catalog_state.clone())),
            ("secrets", Arc::new(secrets_state.clone())),
            ("auth", Arc::new(auth_state.clone())),
        ],
        CONFIG.health_check_frequency_seconds,
        CONFIG.health_check_jitter_millis,
    );
    health_provider.spawn_health_checks().await;

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
        auth_state,
        catalog_state,
        secrets_state,
        CloudEventsPublisher::new(tx.clone()),
        ContractVerifiers::new(vec![]),
        if let Some(uri) = CONFIG.openid_provider_uri.clone() {
            Some(Verifier::new(uri).await?)
        } else {
            None
        },
        health_provider,
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
            print_info();
            println!("Migrating database...");
            let write_pool = iceberg_catalog::implementations::postgres::get_writer_pool().await?;

            // This embeds database migrations in the application binary so we can ensure the database
            // is migrated correctly on startup

            iceberg_catalog::implementations::postgres::migrate(&write_pool).await?;
            println!("Database migration complete.");
        }
        Some(Commands::Serve {}) => {
            print_info();
            println!("Starting server on 0.0.0.0:8080...");
            let bind_addr = std::net::SocketAddr::from(([0, 0, 0, 0], 8080));
            serve(bind_addr).await?;
        }
        Some(Commands::Healthcheck {
            check_all,
            mut check_db,
            mut check_server,
        }) => {
            check_db |= check_all;
            check_server |= check_all;

            tracing::info!("Checking health...");
            if check_db {
                match db_health_check().await {
                    Ok(_) => {
                        tracing::info!("Database is healthy.");
                    }
                    Err(details) => {
                        tracing::info!(?details, "Database is not healthy.");
                        std::process::exit(1);
                    }
                };
            };

            if check_server {
                let client = reqwest::Client::new();
                let response = client.get("http://localhost:8080/health").send().await?;

                let status = response.status();
                if !status.is_success() {
                    tracing::info!("Server is not healthy: StatusCode: '{}'", status);
                    std::process::exit(1);
                }
                let body = response.json::<HealthState>().await?;
                // Fail with an error if the server is not healthy
                if !matches!(body.health, HealthStatus::Healthy) {
                    tracing::info!(?body, "Server is not healthy: StatusCode: '{}'", status,);
                    std::process::exit(1);
                } else {
                    tracing::info!("Server is healthy.");
                }
            }
        }
        Some(Commands::Version {}) => {
            println!("{}", env!("CARGO_PKG_VERSION"));
        }
        None => {
            // Error out if no subcommand is provided.
            eprintln!("No subcommand provided. Use --help for more information.");
        }
        Some(Commands::EnsureMigrate {}) => {
            print_info();
            tracing::info!("Checking if database is migrated...");

            let read_pool = iceberg_catalog::implementations::postgres::get_reader_pool().await?;
            iceberg_catalog::implementations::postgres::ensure_migrations_applied(&read_pool)
                .await?;

            tracing::info!("Database migration complete.");
        }

        Some(Commands::ManagementOpenapi {}) => {
            use utoipa::OpenApi;
            println!("{}", ManagementApiDoc::openapi().to_yaml()?)
        }
    }

    Ok(())
}

async fn db_health_check() -> anyhow::Result<()> {
    let reader = get_reader_pool()
        .await
        .with_context(|| "Read pool failed.")?;
    let writer = get_writer_pool()
        .await
        .with_context(|| "Write pool failed.")?;

    let db = ReadWrite::from_pools(reader.clone(), writer.clone());
    db.update_health().await;
    db.health().await;
    let mut db_healthy = true;

    for h in db.health().await {
        tracing::info!("{:?}", h);
        db_healthy = db_healthy && matches!(h.status(), HealthStatus::Healthy);
    }
    if db_healthy {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Database is not healthy."))
    }
}

fn print_info() {
    println!("Iceberg Catalog Version: {}", env!("CARGO_PKG_VERSION"));
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
