FROM rust:1.90-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --locked --release --example swarm

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 swarm
COPY --from=build /src/target/release/examples/swarm /usr/local/bin/swarm
USER swarm
ENTRYPOINT ["swarm"]
CMD ["--port", "8088"]
