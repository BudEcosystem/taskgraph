use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, Content, ErrorData, Implementation, ListToolsResult,
    PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::RequestContext;
use rmcp::RoleServer;
use serde_json::Value;
use std::sync::Arc;

use crate::db::Database;
use crate::mcp::tools;

#[derive(Clone)]
pub struct TaskgraphMcpHandler {
    db: Arc<Database>,
}

impl TaskgraphMcpHandler {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

impl ServerHandler for TaskgraphMcpHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("taskgraph", env!("CARGO_PKG_VERSION"))
                    .with_title("Taskgraph MCP Server")
                    .with_description("Task graph primitive for AI agent orchestration"),
            )
            .with_instructions(
                "Task graph server for managing task dependency graphs. \
                 Use tools to create projects, add tasks with dependencies, \
                 and execute them in dependency order.",
            )
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let schemas = tools::tool_schemas();
        let mut tool_list = Vec::with_capacity(schemas.len());
        for schema in schemas {
            let tool: Tool = serde_json::from_value(schema).map_err(|e| {
                ErrorData::internal_error(
                    format!("failed to convert tool schema: {e}"),
                    None,
                )
            })?;
            tool_list.push(tool);
        }
        Ok(ListToolsResult {
            tools: tool_list,
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let name = &request.name;
        let args = match request.arguments {
            Some(map) => Value::Object(map),
            None => Value::Object(serde_json::Map::new()),
        };

        match tools::call_tool(&self.db, name, args) {
            Ok(value) => {
                let text =
                    serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string());
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            Err(e) => Ok(CallToolResult::error(vec![Content::text(e.to_string())])),
        }
    }
}
