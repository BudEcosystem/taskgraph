# Build stage
FROM rust:1.93-slim-bookworm AS builder

WORKDIR /app

# Cache dependency layer: copy manifests and build with dummy source
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src

# Copy real source and rebuild (only recompiles project code)
COPY src/ src/
RUN touch src/main.rs && cargo build --release

# Runtime stage
FROM debian:bookworm-slim

COPY --from=builder /app/target/release/taskgraph /usr/local/bin/taskgraph

RUN mkdir -p /data

ENV TASKGRAPH_DB=/data/taskgraph.db
ENV RUST_LOG=info

EXPOSE 8484

ENTRYPOINT ["taskgraph"]
CMD ["serve", "--port", "8484"]
