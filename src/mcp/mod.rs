pub mod handler;
pub mod protocol;
pub mod server;
pub mod tools;

use anyhow::Result;
use rmcp::ServiceExt as _;
use std::sync::Arc;

use crate::db::init_db;
use handler::TaskgraphMcpHandler;

pub async fn run_mcp_server(db_path: &str) -> Result<()> {
    let db = Arc::new(init_db(db_path)?);
    let handler = TaskgraphMcpHandler::new(db);
    let service = handler.serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;
    Ok(())
}
