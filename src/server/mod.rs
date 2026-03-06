pub mod routes;
pub mod sse;

use crate::db::{init_db, run_sweep, Database};
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
            tokio::time::sleep(Duration::from_secs(10)).await;
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
