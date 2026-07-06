# taskgraph

taskgraph is a small, SQLite-backed task graph for AI agent orchestration.

It gives agents a shared project plan, a dependency-aware ready queue, atomic task
claiming, result handoff, artifacts, notes, events, and plan adaptation without
running a separate workflow service. For local use, the coordination layer is just
one file: `.taskgraph.db`.

## What taskgraph does

taskgraph turns a plan into a directed graph of tasks:

- tasks have states such as `pending`, `ready`, `claimed`, `running`,
  `sleeping`, `done`, `failed`, and `cancelled`;
- dependencies decide when downstream work becomes ready;
- agents atomically claim ready work so two agents do not pick the same task;
- completed task results are stored as JSON and included as handoff context for
  downstream tasks;
- agents can put owned work into durable sleep, exit to save resources, and
  resume later with an opaque state reference;
- agents can launch external processes under taskgraph observation and wake on
  exit, kill, stuck timeout, or stdout/stderr hook signals;
- notes, artifacts, events, progress, files, retries, timeouts, and approvals are
  tracked beside the task graph;
- plans can be changed while work is in progress with insert, amend, split,
  pivot, decompose, and replan operations.

It is not an agent framework. It does not run models, route work by capability, or
own your prompts. It is the coordination primitive that agents, scripts, MCP
clients, or dashboards can share.

## Who needs it

Use taskgraph when you have:

- an AI coding or research task with multiple ordered steps;
- several agents or scripts that need to coordinate without stepping on each
  other;
- a plan that may change after new information appears;
- agents that wait on external work, downloads, training runs, rate limits, or
  other time-based gaps and should not stay alive just to poll;
- agents that launch local processes and need a durable callback when the process
  finishes, is killed, gets stuck, or emits a known output signal;
- a local-first workflow where a single binary and SQLite file are preferable to
  Redis, a queue, or a workflow server;
- an MCP-compatible editor or agent that needs tools for planning and execution.

You probably want a larger workflow engine if you need thousands of distributed
workers, cross-region timer guarantees, complex worker routing, automatic process
supervision, or strict enterprise workflow guarantees. taskgraph targets small to
medium agent workloads where portability and low operational overhead matter
more.

## How it works

Each task belongs to a project. A project is stored in SQLite with these main
tables:

- `projects`: named containers for task graphs.
- `tasks`: lifecycle state, metadata, result JSON, progress, retry, approval,
  durable sleep, and agent ownership.
- `dependencies`: edges from upstream tasks to downstream tasks.
- `artifacts`: named outputs attached to tasks.
- `task_notes`: comments for inter-agent communication.
- `task_files`: files touched by tasks for conflict checks.
- `process_runs`, `process_hooks`, `task_waits`: durable process observation
  state for launched child processes.
- `notification_outbox`: retryable HTTP callbacks for process events.
- `events`: audit and monitoring log.

Task state flow:

```text
pending -> ready -> claimed -> running -> done
                                      \-> failed
                         \-> sleeping -> running

running/claimed -> ready       via pause
pending/ready/running -> cancelled
```

Readiness is computed by the `task_readiness` SQL view. A task in `pending`
becomes `ready` when all blocking upstream dependencies are complete. Claiming is
an atomic SQLite update against the ready queue, ordered by priority and creation
time.

Sleeping is a durable wait owned by the same logical agent. A sleeping task is
not claimable by other agents and is not reclaimed by heartbeat expiry. When its
wake time arrives, taskgraph records a `task_wake_due` event and exposes the task
through wake inspection APIs; the agent or harness can then resume the task using
the stored `agent_id`.

Dependency kinds:

| Kind | Blocks readiness | Intended use |
| --- | --- | --- |
| `feeds_into` | Yes | Upstream work produces context or artifacts for downstream work. This is the default for `--dep`. |
| `blocks` | Yes | Ordering constraint where the downstream task should wait. |
| `suggests` | No | Soft relationship used for context, not scheduling. |

Task kinds:

```text
generic, code, research, review, test, shell
```

IDs are short and human-typed, such as `p-ab12cd` for projects and `t-k9x2pq`
for tasks.

## Install

Install the latest release:

```sh
curl -fsSL https://raw.githubusercontent.com/BudEcosystem/taskgraph/main/install.sh | sh
```

Build from source:

```sh
cargo build --release
./target/release/taskgraph --version
```

Run with Docker:

```sh
docker compose up
```

The Docker image starts the HTTP server on port `8484` and stores the database in
the `taskgraph-data` volume.

## Quick start

Create a project. This also sets it as the default project for later commands:

```sh
taskgraph init "auth-system" --description "Implement auth for the API"
```

Add tasks. A task with no dependencies becomes ready immediately. A task with a
dependency stays pending until the upstream task is done:

```sh
taskgraph add --title "Design auth schema" --kind research
taskgraph add --title "Implement auth endpoints" --kind code --dep <DESIGN_TASK_ID>
taskgraph add --title "Write integration tests" --kind test --dep <IMPLEMENT_TASK_ID>
```

Use the real task IDs printed by the previous commands. Dependency syntax is
`--dep TASK_ID` for the default `feeds_into` edge, or `--dep TASK_ID:blocks`,
`--dep TASK_ID:feeds_into`, or `--dep TASK_ID:suggests`.

Claim and start the next ready task:

```sh
taskgraph go --agent agent-1
```

Complete the task, pass result data to downstream tasks, record changed files, and
optionally claim the next task in one command:

```sh
taskgraph done t-k9x2pq \
  --result '{"summary":"schema designed","tables":["users","sessions"]}' \
  --files "docs/auth-schema.md" \
  --next \
  --agent agent-1
```

Check progress:

```sh
taskgraph status
taskgraph status --detail
taskgraph task overview --json -c
```

## Core workflow for agents

The recommended loop is intentionally small:

```sh
taskgraph go --agent <agent-name>
# do the work returned by taskgraph
taskgraph done <task-id> --result '<json-or-text>' --next --agent <agent-name>
```

`go` returns the task, upstream handoff context, notes, file conflict information,
and remaining project counts. `done --next` completes the current task and calls
`go` again for the same agent.

For more precise control, use the plumbing commands:

```sh
taskgraph task next --agent agent-1
taskgraph task claim t-k9x2pq --agent agent-1
taskgraph task start t-k9x2pq
taskgraph task heartbeat t-k9x2pq
taskgraph task progress t-k9x2pq --percent 50 --note "halfway"
taskgraph task done t-k9x2pq --result '{"ok":true}'
```

## Durable sleep and wake

Use durable sleep when an agent owns a task but has nothing useful to do until a
time-based wait expires. Examples include a model download, a training job, a
remote API cooldown, or an external batch operation.

```sh
taskgraph go --agent trainer-1

# Save any large or framework-specific state outside taskgraph, then store a
# compact reference to it. Bare numbers are milliseconds; suffixes support ms/s/m/h.
taskgraph sleep t-k9x2pq 3000 \
  --state-ref '{"checkpoint":"hf-download-42","path":"/tmp/model"}' \
  --reason "waiting for Hugging Face download"
```

`sleep` captures the current task's `agent_id`; the agent does not register
itself separately. The task moves to `sleeping`, receives a `sleep_id`, stores
the opaque `state_ref`, clears its heartbeat, and can safely exit. The state
reference is intentionally opaque: taskgraph stores enough JSON to resume the
agent, while the agent framework owns any large checkpoints, process state, model
files, or protocol-specific data.

When the wake time arrives, `taskgraph serve` emits one `task_wake_due` event per
sleep cycle:

```sh
taskgraph events watch --project p-ab12cd --type task_wake_due
```

Without the server, a harness can still poll the durable state:

```sh
taskgraph wakes due --project p-ab12cd
```

Resume with the same logical agent after the wake time is due. Early resume
attempts are rejected. Pass `sleep_id` when you want to reject stale resume
attempts from an older sleep cycle:

```sh
taskgraph resume t-k9x2pq --agent trainer-1 --sleep-id s-a1b2c3
```

After resume, the task returns to `running` and the stored `state_ref` is returned
to the caller. Downstream scheduling still depends on `done`; sleep only suspends
the current owner. Wake times are stored with millisecond precision.

## Process observation

Use process observation when an agent owns a task and wants taskgraph to launch a
child process, observe it after the agent exits, and wake the same logical agent
when something meaningful happens.

```sh
taskgraph go --agent trainer-1

taskgraph process launch t-k9x2pq \
  --agent trainer-1 \
  --hook model_ready=stdout:MODEL_READY \
  --callback http://127.0.0.1:9000/taskgraph \
  --state-ref '{"checkpoint":"hf-download-42"}' \
  -- python download_model.py --model x
```

`process launch` creates a `process_run`, creates a process `task_wait`, moves the
task to `sleeping`, and starts a detached taskgraph runner. The runner launches
the child process, captures bounded stdout/stderr logs, watches configured hooks,
records exit/killed/stuck states, and dispatches callback notifications through
the SQLite outbox.

When a hook or terminal process state happens, taskgraph marks the wait due and
exposes it through the same wake/resume flow:

```sh
taskgraph wakes due --project p-ab12cd
taskgraph resume t-k9x2pq --agent trainer-1 --sleep-id w-a1b2c3
```

Callbacks are delivered at least once with an `Idempotency-Key` header of
`taskgraph:event:<event_id>`. The outbox leases callback rows before dispatch so
multiple taskgraph processes do not intentionally send the same pending callback
at the same time, but callback consumers should still be idempotent because
network failures can make any at-least-once delivery ambiguous.

If the detached runner stops heartbeating, `taskgraph serve` marks the process
wait due instead of leaving the task asleep forever. A stale runner that never
started becomes `stuck` with reason `runner_missing`; a lost observer becomes
`stuck` with reason `observer_lost` or `runner_unresponsive`; a stale kill request
becomes `killed`. Taskgraph kills an orphan child process when it can, because it
can no longer truthfully observe hooks or exit status after the observer is lost.

Remote process launch is disabled by default. Start the HTTP server with
`taskgraph serve --enable-process-launch` to allow `POST /api/tasks/{id}/processes`
and HTTP MCP process launch. For stdio MCP, set
`TASKGRAPH_ENABLE_PROCESS_LAUNCH=1` only when that local MCP client is trusted to
run commands on the host.

## Interfaces

taskgraph exposes the same SQLite-backed graph through three interfaces.

### CLI

The CLI is best for local agents, shell scripts, and humans:

```sh
taskgraph --help
taskgraph project create "my-project"
taskgraph add --title "First task"
taskgraph go --agent cli-agent
```

Useful global options:

| Option | Description |
| --- | --- |
| `--db <PATH>` | SQLite database path. Defaults to `.taskgraph.db`. |
| `--json` | Print structured JSON. |
| `-c`, `--compact` | Print token-efficient output for LLM context windows. |

### MCP server

Use MCP when you want Claude Code, Cursor, Windsurf, or another MCP client to call
taskgraph tools directly:

```sh
taskgraph mcp
```

Example MCP config:

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

You can also generate ready-to-paste integration text:

```sh
taskgraph prompt --list
taskgraph prompt --for mcp
taskgraph prompt --for cli
taskgraph prompt --for http
```

### HTTP API and SSE

Start the server:

```sh
taskgraph serve --port 8484
```

Available surfaces:

| Surface | URL |
| --- | --- |
| REST API | `http://localhost:8484/api` |
| Event stream | `http://localhost:8484/api/events/stream` |
| MCP over HTTP | `http://localhost:8484/mcp` |

Example HTTP calls:

```sh
curl -s http://localhost:8484/api/projects

curl -s -X POST http://localhost:8484/api/go \
  -H 'content-type: application/json' \
  -d '{"project_id":"p-ab12cd","agent_id":"agent-1"}'

curl -s -X POST http://localhost:8484/api/tasks/t-k9x2pq/done \
  -H 'content-type: application/json' \
  -d '{"result":{"summary":"done"},"next":true,"agent_id":"agent-1"}'

curl -s -X POST http://localhost:8484/api/tasks/t-k9x2pq/sleep \
  -H 'content-type: application/json' \
  -d '{"duration":"3s","state_ref":{"checkpoint":"download-42"},"reason":"waiting"}'

curl -s 'http://localhost:8484/api/wakes/due?project=p-ab12cd'

curl -s -X POST http://localhost:8484/api/tasks/t-k9x2pq/resume \
  -H 'content-type: application/json' \
  -d '{"agent_id":"agent-1","sleep_id":"s-a1b2c3"}'
```

The HTTP server runs a background sweeper that periodically promotes ready tasks,
reclaims stale work, handles timeouts, emits due wake events, and rolls up
composite tasks.

## CLI command reference

### Project commands

| Command | Purpose |
| --- | --- |
| `taskgraph init <name>` | Create a project and set it as default. |
| `taskgraph project create <name>` | Create a project and set it as default. |
| `taskgraph project list` | List projects. |
| `taskgraph project status [project_id]` | Show counts for a project. |
| `taskgraph project dag [project_id]` | Render the dependency graph as a tree. |
| `taskgraph use [project_id]` | Show or set the default project. |
| `taskgraph use --clear` | Clear the default project. |

### Task commands

| Command | Purpose |
| --- | --- |
| `taskgraph add --title ...` | Shortcut for `taskgraph task create`. |
| `taskgraph task create` | Create one task. |
| `taskgraph task create-batch --file tasks.yaml` | Create many tasks from YAML. |
| `taskgraph list` | Shortcut for `taskgraph task list`. |
| `taskgraph task list` | List tasks with filters. |
| `taskgraph show <task_id>` | Shortcut for `taskgraph task get`. |
| `taskgraph task get <task_id>` | Show a task. Supports fuzzy ID matching. |
| `taskgraph task next --agent <name>` | Peek or claim the next ready task. |
| `taskgraph go --agent <name>` | Claim and start the next ready task. |
| `taskgraph task claim <task_id> --agent <name>` | Claim a specific ready task. |
| `taskgraph task start <task_id>` | Mark claimed work as running. |
| `taskgraph task heartbeat <task_id>` | Update liveness for claimed/running work. |
| `taskgraph task progress <task_id> --percent N` | Save progress and an optional note. |
| `taskgraph sleep <task_id> <duration>` | Put owned work into durable sleep. |
| `taskgraph task sleep <task_id> <duration>` | Same as `sleep`; accepts `--state-ref` and `--reason`. |
| `taskgraph wakes due [--project ...]` | List sleeping tasks whose wake time has arrived. |
| `taskgraph resume <task_id> --agent <name>` | Resume a sleeping task for the same logical agent. |
| `taskgraph task resume <task_id> --agent <name>` | Same as `resume`; accepts `--sleep-id`. |
| `taskgraph process launch <task_id> --agent ... -- <cmd>` | Launch and observe a process, then wake on hook/exit/killed/stuck. |
| `taskgraph process get <run_id>` | Inspect a process run. |
| `taskgraph process logs <run_id>` | Read captured stdout/stderr. |
| `taskgraph process kill <run_id>` | Request termination of a running process. |
| `taskgraph done <task_id>` | Shortcut for `taskgraph task done`. |
| `taskgraph task done <task_id>` | Complete a task, optionally with result JSON. |
| `taskgraph task fail <task_id> --error ...` | Mark a running task as failed. |
| `taskgraph task pause <task_id>` | Return claimed/running work to ready with progress. |
| `taskgraph task cancel <task_id> [--cascade]` | Cancel a task, optionally downstream tasks too. |
| `taskgraph task approve <task_id>` | Approve work that requires human approval. |
| `taskgraph task update <task_id>` | Update title, description, kind, or priority. |
| `taskgraph task add-dep <to_task> --after <from_task>` | Add a dependency edge. |
| `taskgraph task remove-dep <to_task> --after <from_task>` | Remove a dependency edge. |
| `taskgraph task note <task_id> "text"` | Add a task note. |
| `taskgraph task notes <task_id>` | List notes for a task. |
| `taskgraph task overview` | Show all tasks, dependencies, and summary. |

Task list filters:

```sh
taskgraph task list --status ready
taskgraph task list --kind code
taskgraph task list --tag backend
taskgraph task list --agent agent-1
```

Sleep durations accept bare milliseconds or `ms`, `s`, `m`, and `h` suffixes:

```sh
taskgraph sleep t-k9x2pq 3000
taskgraph sleep t-k9x2pq 3s --state-ref '{"external_job":"download-42"}'
taskgraph resume t-k9x2pq --agent agent-1 --sleep-id s-a1b2c3
taskgraph process launch t-k9x2pq --agent agent-1 --hook ready=stdout:READY -- sh -c 'echo READY'
```

### Plan adaptation commands

| Command | Purpose |
| --- | --- |
| `taskgraph ahead --depth 3` | See running work and upcoming dependency layers. |
| `taskgraph what-if cancel <task_id>` | Preview cancellation effects without changing the graph. |
| `taskgraph what-if insert --after A --before B --title ...` | Preview insertion effects. |
| `taskgraph task insert --after A --before B --title ...` | Insert a new task between existing tasks and rewire edges. |
| `taskgraph task amend <task_id> --prepend "NOTE: ..."` | Add context to a future task description. |
| `taskgraph task split <task_id> --into '[...]'` | Split one task into executable parts. |
| `taskgraph task decompose <task_id> --file subtasks.yaml` | Turn a task into a composite parent with subtasks. |
| `taskgraph task replan <task_id> --file subtasks.yaml` | Cancel remaining pending subtasks and create replacements. |
| `taskgraph task pivot <task_id> --file new-plan.yaml` | Replace a task subtree. |

Example `split` JSON:

```sh
taskgraph task split t-parent \
  --into '[{"title":"Add model","description":"Create data model"},{"title":"Add API","deps_on":["Add model"]}]'
```

Example decomposition YAML:

```yaml
subtasks:
  - title: "Design schema"
    kind: research
  - title: "Implement migrations"
    kind: code
    deps_on: ["Design schema"]
  - title: "Test migrations"
    kind: test
    deps_on: ["Implement migrations"]
```

### Artifacts

Artifacts are named outputs attached to a task. Use them for reports, generated
configs, snippets, or file-backed outputs.

```sh
taskgraph artifact write --task t-k9x2pq --name schema --file docs/schema.md --kind report
taskgraph artifact write --task t-k9x2pq --name summary --content '{"ok":true}' --mime application/json
taskgraph artifact list --task t-k9x2pq
taskgraph artifact read --task t-k9x2pq --name schema
```

### Events

Events are emitted for task lifecycle changes, dependency changes, artifacts, and
approval resolution.

```sh
taskgraph events list --project p-ab12cd
taskgraph events list --project p-ab12cd --type task_completed --limit 20
taskgraph events watch --project p-ab12cd
```

Event types include:

```text
task_created, task_ready, task_claimed, task_started, task_sleeping,
task_wake_due, task_resumed, task_completed, task_failed, task_retrying,
task_cancelled, dependency_added, artifact_created, approval_requested,
approval_resolved
```

## MCP tools

MCP tools mirror the CLI and return JSON strings. The main tools are:

| Area | Tools |
| --- | --- |
| Projects | `taskgraph_project_create`, `taskgraph_project_status`, `taskgraph_project_dag`, `taskgraph_project_overview`, `taskgraph_status` |
| Task creation | `taskgraph_task_create`, `taskgraph_task_create_batch`, `taskgraph_task_decompose`, `taskgraph_task_replan` |
| Work loop | `taskgraph_go`, `taskgraph_task_next`, `taskgraph_task_claim`, `taskgraph_task_start`, `taskgraph_task_sleep`, `taskgraph_task_resume`, `taskgraph_task_done` |
| Task state | `taskgraph_task_fail`, `taskgraph_task_pause`, `taskgraph_task_update`, `taskgraph_task_get_context`, `taskgraph_wakes_due` |
| Processes | `taskgraph_process_launch`, `taskgraph_process_get`, `taskgraph_process_list`, `taskgraph_process_kill`, `taskgraph_process_logs` |
| Dependencies | `taskgraph_dependency_add`, `taskgraph_dependency_remove` |
| Adaptation | `taskgraph_what_if`, `taskgraph_task_insert`, `taskgraph_ahead`, `taskgraph_task_amend`, `taskgraph_task_pivot`, `taskgraph_task_split` |
| Collaboration | `taskgraph_task_note`, `taskgraph_task_notes` |
| Artifacts | `taskgraph_artifact_write`, `taskgraph_artifact_read` |

Typical MCP flow:

1. `taskgraph_project_create`
2. `taskgraph_task_create` for each planned task
3. `taskgraph_go` to claim work
4. `taskgraph_task_sleep` when the owning agent should save state and exit until a wake time
5. `taskgraph_process_launch` when the wait is a child process that taskgraph should observe
6. `taskgraph_task_resume` when `taskgraph_wakes_due` or `task_wake_due` says the wait is due
7. `taskgraph_task_done` with `result` and optionally `next: true`
8. `taskgraph_status` or `taskgraph_project_overview` for progress

## HTTP API reference

All REST routes are under `/api`.

| Method and path | Purpose |
| --- | --- |
| `POST /api/projects` | Create a project. |
| `GET /api/projects` | List projects. |
| `GET /api/projects/{id}` | Get a project. |
| `PATCH /api/projects/{id}` | Update project status. |
| `GET /api/projects/{id}/status` | Project counts and progress. |
| `GET /api/projects/{id}/dag` | Task graph nodes and edges. |
| `GET /api/projects/{id}/overview` | Summary, ready IDs, tasks, and edges. |
| `POST /api/projects/{project_id}/tasks` | Create a task. |
| `GET /api/projects/{project_id}/tasks` | List tasks. |
| `POST /api/projects/{project_id}/tasks/batch` | Batch-create tasks. |
| `GET /api/projects/{project_id}/events` | List events. |
| `POST /api/go` | Claim and start next task. |
| `POST /api/tasks/next` | Get or claim the next ready task. |
| `GET /api/tasks/{id}` | Get a task. |
| `PATCH /api/tasks/{id}` | Update task fields. |
| `GET /api/tasks/{id}/context` | Task, project, upstream artifacts, downstream tasks, siblings. |
| `POST /api/tasks/{id}/claim` | Claim a task. |
| `POST /api/tasks/{id}/start` | Start a claimed task. |
| `POST /api/tasks/{id}/heartbeat` | Update heartbeat. |
| `POST /api/tasks/{id}/progress` | Update progress. |
| `POST /api/tasks/{id}/sleep` | Put owned work into durable sleep. |
| `POST /api/tasks/{id}/resume` | Resume a sleeping task for the same logical agent. |
| `POST /api/tasks/{id}/processes` | Launch and observe a process for an owned task. Requires `--enable-process-launch`. |
| `GET /api/processes/{id}` | Get process run status. |
| `POST /api/processes/{id}/kill` | Request process termination. |
| `GET /api/processes/{id}/logs` | Read captured stdout/stderr logs. |
| `POST /api/tasks/{id}/done` | Complete a task. |
| `POST /api/tasks/{id}/pause` | Pause a task. |
| `POST /api/tasks/{id}/fail` | Fail a task. |
| `POST /api/tasks/{id}/cancel` | Cancel a task. |
| `POST /api/tasks/{id}/approve` | Approve a task. |
| `POST /api/tasks/{id}/notes` | Add a note. |
| `GET /api/tasks/{id}/notes` | List notes. |
| `POST /api/tasks/{id}/deps` | Add a dependency. |
| `DELETE /api/tasks/{id}/deps` | Remove a dependency. |
| `POST /api/tasks/{id}/decompose` | Decompose into subtasks. |
| `POST /api/tasks/{id}/replan` | Replan subtasks. |
| `POST /api/tasks/{id}/amend` | Prepend context to a task. |
| `POST /api/tasks/{id}/pivot` | Replace a subtree. |
| `POST /api/tasks/{id}/split` | Split a task. |
| `POST /api/tasks/{task_id}/artifacts` | Create an artifact. |
| `GET /api/tasks/{task_id}/artifacts` | List artifacts. |
| `GET /api/tasks/{task_id}/upstream-artifacts` | List artifacts from upstream tasks. |
| `GET /api/artifacts/{id}` | Get an artifact. |
| `GET /api/ahead?project=...&depth=2` | Look ahead in the graph. |
| `GET /api/wakes/due?project=...` | List sleeping tasks whose wake time has arrived. |
| `POST /api/what-if` | Dry-run a graph mutation. |
| `GET /api/events/stream` | Server-sent event stream. |

## Batch YAML

Create multiple top-level tasks:

```yaml
tasks:
  - id: design
    title: "Design auth schema"
    kind: research
    priority: 10
    tags: [auth, backend]
  - id: implement
    title: "Implement auth endpoints"
    kind: code
    deps:
      - from: design
        kind: feeds_into
  - title: "Review auth implementation"
    kind: review
    deps:
      - from: implement
        kind: blocks
```

Load it:

```sh
taskgraph task create-batch --file tasks.yaml
```

## Configuration

| Setting | Default | Description |
| --- | --- | --- |
| `--db <PATH>` | `.taskgraph.db` | Database path for this command. |
| `TASKGRAPH_DB` | unset | Database path used when `--db` is omitted. |
| `TASKGRAPH_ENABLE_PROCESS_LAUNCH` | unset | Enables process launch through MCP; HTTP also requires `serve --enable-process-launch`. |
| `TASKGRAPH_PROCESS_RUNNER_STALE_MS` | `15000` | Heartbeat grace window before a process runner is considered lost. Minimum accepted value is `1000`. |
| `TASKGRAPH_PROCESS_LOG_MAX_BYTES` | `10485760` | Max captured log bytes per process stream before the runner truncates capture. |
| `TASKGRAPH_PROCESS_LOG_READ_MAX_BYTES` | `262144` | Max bytes returned per stream by `process logs`; larger files return the tail with truncation flags. |
| `RUST_LOG` | unset | Logging filter, for example `info` or `debug`. |
| `NO_COLOR` | unset | Disable colored terminal output when set. |
| `INSTALL_DIR` | `/usr/local/bin` | Install location used by `install.sh`. |

## Operational notes

- The SQLite database is the source of truth. You can copy, back up, or inspect
  `.taskgraph.db` directly.
- WAL mode is enabled so readers can continue while another process writes.
- Atomic claims rely on SQLite write serialization.
- `taskgraph serve` runs the background sweeper. It promotes ready tasks,
  reclaims stale work, handles timeouts, reaps stale process observers, dispatches
  due callbacks, and emits near-exact `task_wake_due` events for sleeping tasks.
- Without relying on background wake events, durable sleep state is still stored
  in SQLite and can be discovered with `taskgraph wakes due`. HTTP clients can
  use `GET /api/wakes/due` when the server is running.
- Sleeping tasks keep their `agent_id`, are not claimable by other agents, and are
  not reclaimed by heartbeat expiry.
- Use `--json -c` when an LLM will consume the output.
- Use `taskgraph prompt --for cli` or `taskgraph prompt --for mcp` to generate
  agent instructions that match the installed binary.

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the detailed design: state
machine, dependency engine, handoff protocol, event system, and SQLite trade-offs.

## License

Apache-2.0
