use clap::{Parser, Subcommand};
use iceberg_catalog::api::management::v1::ManagementApiDoc;
use iceberg_catalog::CONFIG;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::EnvFilter;

mod healthcheck;
mod serve;
mod wait_for_db;

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
    /// Wait for the database to be up and migrated
    WaitForDB {
        #[clap(
            default_value = "false",
            short = 'd',
            help = "Test DB connection, requires postgres env values."
        )]
        check_db: bool,
        #[clap(
            default_value = "false",
            short = 'm',
            help = "Check migrations, implies -d."
        )]
        check_migrations: bool,
        #[clap(
            default_value_t = 15,
            long,
            short,
            help = "Number of retries to connect to the database, implies -w."
        )]
        retries: u32,
        #[clap(
            default_value_t = 2,
            long,
            short,
            help = "Delay in seconds between retries to connect to the database."
        )]
        backoff: u64,
    },
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
        Some(Commands::WaitForDB {
            check_db,
            check_migrations,
            retries,
            backoff,
        }) => {
            let check_db = check_db || check_migrations;

            wait_for_db::wait_for_db(check_migrations, retries, backoff, check_db).await?;
        }
        Some(Commands::Migrate {}) => {
            print_info();
            println!("Migrating database...");
            let write_pool = iceberg_catalog::implementations::postgres::get_writer_pool(
                CONFIG
                    .to_pool_opts()
                    .acquire_timeout(std::time::Duration::from_secs(1)),
            )
            .await?;

            // This embeds database migrations in the application binary so we can ensure the database
            // is migrated correctly on startup

            iceberg_catalog::implementations::postgres::migrate(&write_pool).await?;
            println!("Database migration complete.");
        }
        Some(Commands::Serve {}) => {
            print_info();
            tracing::info!("Starting server on 0.0.0.0:{}...", CONFIG.listen_port);
            let bind_addr = std::net::SocketAddr::from(([0, 0, 0, 0], CONFIG.listen_port));
            serve::serve(bind_addr).await?;
        }
        Some(Commands::Healthcheck {
            check_all,
            mut check_db,
            mut check_server,
        }) => {
            check_db |= check_all;
            check_server |= check_all;
            healthcheck::health(check_db, check_server).await?;
        }
        Some(Commands::Version {}) => {
            println!("{}", env!("CARGO_PKG_VERSION"));
        }
        Some(Commands::ManagementOpenapi {}) => {
            use utoipa::OpenApi;
            println!("{}", ManagementApiDoc::openapi().to_yaml()?)
        }
        None => {
            // Error out if no subcommand is provided.
            eprintln!("No subcommand provided. Use --help for more information.");
        }
    }

    Ok(())
}

fn print_info() {
    tracing::info!("Iceberg Catalog Version: {}", env!("CARGO_PKG_VERSION"));
}
