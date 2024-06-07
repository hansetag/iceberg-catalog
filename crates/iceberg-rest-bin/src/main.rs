use clap::{Parser, Subcommand};
use iceberg_rest_server::service::event_publisher::TracingPublisher;
use iceberg_rest_server::{
    implementations::{
        postgres::{Catalog, CatalogState, SecretsState, SecretsStore},
        AllowAllAuthState, AllowAllAuthZHandler,
    },
    service::router::{new_full_router, serve as service_serve},
};

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
    let read_pool = iceberg_rest_server::implementations::postgres::get_reader_pool().await?;
    let write_pool = iceberg_rest_server::implementations::postgres::get_writer_pool().await?;

    let catalog_state = CatalogState {
        read_pool: read_pool.clone(),
        write_pool: write_pool.clone(),
    };
    let secrets_state = SecretsState {
        read_pool,
        write_pool,
    };

    let tracing_publisher = TracingPublisher;

    let router = new_full_router::<
        Catalog,
        Catalog,
        AllowAllAuthZHandler,
        AllowAllAuthZHandler,
        SecretsStore,
        TracingPublisher,
    >(
        AllowAllAuthState,
        catalog_state,
        secrets_state,
        tracing_publisher,
    );

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;

    service_serve(listener, router).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    env_logger::init();

    match cli.command {
        Some(Commands::Migrate {}) => {
            println!("Migrating database...");
            let write_pool =
                iceberg_rest_server::implementations::postgres::get_writer_pool().await?;

            // This embeds database migrations in the application binary so we can ensure the database
            // is migrated correctly on startup
            iceberg_rest_server::implementations::postgres::migrate(&write_pool).await?;
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
