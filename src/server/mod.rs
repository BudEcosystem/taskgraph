pub mod routes;
pub mod sse;

use crate::db::{dispatch_due_notifications, init_db, next_sleep_due_at, run_sweep, Database};
use crate::mcp::handler::TaskgraphMcpHandler;
use anyhow::Result;
use axum::Router;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::StreamableHttpServerConfig;
use rmcp::transport::streamable_http_server::StreamableHttpService;
use std::sync::Arc;
use std::time::Duration;
use tower_http::cors::CorsLayer;

pub use routes::api_routes;

pub async fn run_server(db_path: &str, port: u16) -> Result<()> {
    let db = Arc::new(init_db(db_path)?);

    let sweep_db: Arc<Database> = db.clone();
    tokio::spawn(async move {
        loop {
            let _ = run_sweep(&sweep_db);
            let _ = dispatch_due_notifications(&sweep_db, 32);
            let sleep_for = match next_sleep_due_at(&sweep_db) {
                Ok(Some(next_due)) => {
                    let now = chrono::Utc::now().naive_utc();
                    let millis = (next_due - now).num_milliseconds().max(100) as u64;
                    Duration::from_millis(millis.min(1_000))
                }
                _ => Duration::from_secs(1),
            };
            tokio::time::sleep(sleep_for).await;
        }
    });

    // MCP Streamable HTTP service
    let mcp_db = db.clone();
    let mcp_service = StreamableHttpService::new(
        move || Ok(TaskgraphMcpHandler::new(mcp_db.clone())),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig {
            // Disable priming events — the empty SSE data field they produce
            // (`data: \n`) causes JSON parse errors in Python MCP clients that
            // don't expect non-JSON SSE events.
            sse_retry: None,
            ..Default::default()
        },
    );

    let app = Router::new()
        .nest("/api", api_routes())
        .nest_service("/mcp", mcp_service)
        .layer(CorsLayer::permissive())
        .with_state(db);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    println!("Taskgraph server listening on http://0.0.0.0:{port}");
    println!("  REST API: http://0.0.0.0:{port}/api");
    println!("  MCP HTTP: http://0.0.0.0:{port}/mcp");
    axum::serve(listener, app).await?;
    Ok(())
}
