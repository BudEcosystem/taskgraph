# taskgraph

A task graph primitive for AI agent orchestration. Zero-config, embedded, powered by SQLite.

Most agent workloads don't need a separate orchestration service—they just need a file.

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/BudEcosystem/taskgraph/main/install.sh | sh
```

Or build from source:

```sh
cargo build --release
# binary: target/release/taskgraph
```

## Quick start

```sh
# Create a project
taskgraph project create "my-project"

# Add tasks with dependencies
taskgraph add --title "Design API" --kind research
taskgraph add --title "Implement endpoints" --kind code --dep feeds_into:<DESIGN_ID>
taskgraph add --title "Write tests" --kind test --dep feeds_into:<IMPL_ID>

# Claim the next ready task and start working
taskgraph go --agent agent-1

# Complete the task and auto-claim the next one
taskgraph done <TASK_ID> --result '{"schema": "v1"}' --next --agent agent-1

# Check progress
taskgraph status
```

## How it works

taskgraph manages a dependency-aware task graph stored in a single SQLite file (`.taskgraph.db`). Agents claim tasks atomically, complete them, and results flow downstream through the handoff protocol.

**Task lifecycle:**

```
pending → ready → claimed → running → done
                                    → failed
```

Tasks are automatically promoted from `pending` to `ready` when all blocking dependencies are satisfied.

**Dependency types:**

| Type | Meaning |
|------|---------|
| `feeds_into` | Passes result data to downstream task |
| `blocks` | Ordering constraint only |
| `suggests` | Soft dependency (doesn't block promotion) |

**Multi-agent coordination:** Atomic claim ensures no two agents grab the same task. Any agent can claim any ready task—routing logic belongs in your agent framework, not here.

## Three interfaces

### CLI

```sh
taskgraph go --agent agent-1
taskgraph done <ID> --next --agent agent-1
taskgraph status
```

Porcelain commands (`go`, `done`, `add`, `list`, `status`) for common workflows. Plumbing commands (`claim`, `start`, `complete`, `heartbeat`) for precise control.

### MCP server

For Claude Code, Cursor, Windsurf, and other MCP-compatible tools:

```sh
taskgraph mcp
```

Add to your MCP config:

```json
{
  "mcpServers": {
    "taskgraph": {
      "command": "taskgraph",
      "args": ["mcp"]
    }
  }
}
```

### HTTP API

```sh
taskgraph serve --port 8484
```

REST endpoints at `/api/*`, SSE event stream at `/events`, and MCP-over-HTTP at `/mcp`.

## Plan adaptation

Modify the plan mid-execution with six primitives:

| Command | What it does |
|---------|-------------|
| `insert` | Add a step between two tasks |
| `amend` | Prepend context to a future task |
| `split` | Break one task into multiple |
| `pivot` | Replace a subtree with new tasks |
| `decompose` | Create subtasks from YAML |
| `replan` | Replace a subtree with a new YAML plan |

## Docker

```sh
docker compose up
# API available at http://localhost:8484
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `--db <PATH>` | `.taskgraph.db` | Database file path |
| `TASKGRAPH_DB` | — | Database path (env var) |
| `RUST_LOG` | — | Log level (`info`, `debug`) |
| `--json` | — | Structured JSON output |
| `-c, --compact` | — | Token-efficient output for LLMs |

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for design details including the task state machine, dependency engine, handoff protocol, and why SQLite.

## License

Apache-2.0
