#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate slog_async;

use std::env;
use std::sync::Arc;

use actix_cors::Cors;
use actix_web::{middleware, HttpServer};
use meilisearch_http::helpers::NormalizePath;
use meilisearch_http::{create_app_raft, index_update_callback, Data, Opt};
use structopt::StructOpt;
use log::info;
use raft::Raft;

use slog::Drain; 
mod analytics;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // setup runtime for actix
    let local = tokio::task::LocalSet::new();
    let _sys = actix_rt::System::run_in_tokio("server", &local);

    let opt = Opt::from_args();
    let data = Data::new(opt.clone())?;

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, slog_o!("version" => env!("CARGO_PKG_VERSION")));

    // converts log to slog
    let _log_guard = slog_stdlog::init().unwrap();

    let data_cloned = data.clone();
    data.db.set_update_callback(Box::new(move |name, status| {
        index_update_callback(name, &data_cloned, status);
    }));

    let raft = Raft::new(opt.raft_addr.clone(), data.clone(), logger.clone());
    let mailbox = Arc::new(raft.mailbox());
    let raft_handle = match opt.peer_addr.clone() {
        Some(addr) => {
            info!("running in follower mode");
            let handle = tokio::spawn(raft.join(addr));
            handle
        }
        None => {
            info!("running in leader mode");
            let handle =  tokio::spawn(raft.lead());
            handle
        }
    };

    print_launch_resume(&opt, &data);

    let http_server = HttpServer::new(move || {
        create_app_raft(&data, mailbox.clone())
            .wrap(
                Cors::new()
                    .send_wildcard()
                    .allowed_headers(vec!["content-type", "x-meili-api-key"])
                    .max_age(86_400) // 24h
                    .finish(),
            )
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .wrap(NormalizePath)
    });

    if let Some(config) = opt.get_ssl_config()? {
        http_server
            .bind_rustls(opt.http_addr, config)?
            .run()
            .await?;
    } else {
        http_server.bind(opt.http_addr)?.run().await?;
    }

    let _ = tokio::join!(raft_handle);

    Ok(())
}

pub fn print_launch_resume(opt: &Opt, data: &Data) {
    let ascii_name = r#"
888b     d888          d8b 888 d8b  .d8888b.                                    888
8888b   d8888          Y8P 888 Y8P d88P  Y88b                                   888
88888b.d88888              888     Y88b.                                        888
888Y88888P888  .d88b.  888 888 888  "Y888b.    .d88b.   8888b.  888d888 .d8888b 88888b.
888 Y888P 888 d8P  Y8b 888 888 888     "Y88b. d8P  Y8b     "88b 888P"  d88P"    888 "88b
888  Y8P  888 88888888 888 888 888       "888 88888888 .d888888 888    888      888  888
888   "   888 Y8b.     888 888 888 Y88b  d88P Y8b.     888  888 888    Y88b.    888  888
888       888  "Y8888  888 888 888  "Y8888P"   "Y8888  "Y888888 888     "Y8888P 888  888
"#;

    eprintln!("{}", ascii_name);

    eprintln!("Database path:\t\t{:?}", opt.db_path);
    eprintln!("Server listening on:\t{:?}", opt.http_addr);
    eprintln!("Environment:\t\t{:?}", opt.env);
    eprintln!("Commit SHA:\t\t{:?}", env!("VERGEN_SHA").to_string());
    eprintln!(
        "Build date:\t\t{:?}",
        env!("VERGEN_BUILD_TIMESTAMP").to_string()
    );
    eprintln!(
        "Package version:\t{:?}",
        env!("CARGO_PKG_VERSION").to_string()
    );

    #[cfg(all(not(debug_assertions), feature = "sentry"))]
    eprintln!(
        "Sentry DSN:\t\t{:?}",
        if !opt.no_sentry {
            &opt.sentry_dsn
        } else {
            "Disabled"
        }
    );

    eprintln!(
        "Amplitude Analytics:\t{:?}",
        if !opt.no_analytics {
            "Enabled"
        } else {
            "Disabled"
        }
    );

    eprintln!();

    if data.api_keys.master.is_some() {
        eprintln!("A Master Key has been set. Requests to MeiliSearch won't be authorized unless you provide an authentication key.");
    } else {
        eprintln!("No master key found; The server will accept unidentified requests. \
            If you need some protection in development mode, please export a key: export MEILI_MASTER_KEY=xxx");
    }

    eprintln!();
    eprintln!("Documentation:\t\thttps://docs.meilisearch.com");
    eprintln!("Source code:\t\thttps://github.com/meilisearch/meilisearch");
    eprintln!("Contact:\t\thttps://docs.meilisearch.com/resources/contact.html or bonjour@meilisearch.com");
    eprintln!();
}
