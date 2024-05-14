use axum::{http::header::AUTHORIZATION, Router};
use clap::{Parser, Subcommand};
use iceberg_rest_server::{
    implementations::{
        postgres::{Catalog, CatalogState, SecretsState, SecretsStore},
        AllowAllAuthState, AllowAllAuthZHandler,
    },
    service::State as GenericState,
};
use iceberg_rest_service::{shutdown_signal, v1};
use tower_http::{
    catch_panic::CatchPanicLayer, compression::CompressionLayer,
    sensitive_headers::SetSensitiveHeadersLayer, timeout::TimeoutLayer, trace, trace::TraceLayer,
};
use tracing::Level;

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
}

async fn serve(bind_addr: std::net::SocketAddr) -> Result<(), anyhow::Error> {
    type ConfigServer = iceberg_rest_server::catalog::ConfigServer<
        Catalog,
        Catalog,
        AllowAllAuthZHandler,
        AllowAllAuthZHandler,
    >;

    type CatalogServer =
        iceberg_rest_server::catalog::CatalogServer<Catalog, AllowAllAuthZHandler, SecretsStore>;
    type ApiServer =
        iceberg_rest_server::api::ApiServer<Catalog, AllowAllAuthZHandler, SecretsStore>;
    type State = GenericState<AllowAllAuthZHandler, Catalog, SecretsStore>;

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

    let v1_routes = Router::new()
        .merge(v1::config::router::<ConfigServer, State>())
        // .merge(v1::oauth_router::<I, S>())
        .merge(v1::namespace::router::<CatalogServer, State>())
        .merge(v1::tables::router::<CatalogServer, State>())
        .merge(v1::s3_signer::router::<CatalogServer, State>())
        .merge(v1::metrics::router::<CatalogServer, State>())
        // .merge(v1::views_router::<I, S>())
        ;

    let management_routes = Router::new().merge(ApiServer::v1_router());

    // This is the order that the modules were authored in.
    let router = Router::new()
        .nest("/catalog/v1", v1_routes)
        .nest("/management/v1", management_routes)
        // Enables logging. Use `RUST_LOG=tower_http=debug`
        .layer((
            SetSensitiveHeadersLayer::new([AUTHORIZATION]),
            CompressionLayer::new(),
            TraceLayer::new_for_http()
                .on_failure(())
                .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_response(trace::DefaultOnResponse::new().level(Level::DEBUG)),
            TimeoutLayer::new(std::time::Duration::from_secs(30)),
            CatchPanicLayer::new(),
        ))
        .with_state(v1::ApiContext {
            v1_state: State {
                auth: AllowAllAuthState {},
                catalog: catalog_state,
                secrets: secrets_state,
            },
        });

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| anyhow::anyhow!(e).context("error running HTTP server"))?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    env_logger::init();

    match &cli.command {
        Some(Commands::Migrate {}) => {
            println!("Migrating database...");
            let write_pool =
                iceberg_rest_server::implementations::postgres::get_writer_pool().await?;

            // This embeds database migrations in the application binary so we can ensure the database
            // is migrated correctly on startup
            println!("Migrating database...");
            iceberg_rest_server::implementations::postgres::migrate(&write_pool).await?;
            println!("Database migration complete.");
        }
        Some(Commands::Serve {}) => {
            let bind_addr = std::net::SocketAddr::from(([0, 0, 0, 0], 8080));
            serve(bind_addr).await?;
        }
        None => {
            // Error out if no subcommand is provided.
            eprintln!("No subcommand provided. Use --help for more information.");
        }
    }

    Ok(())
}
