FROM rust:1.61-buster as build-env

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential \
    curl \
    openssl libssl-dev \
    pkg-config \
    python \
    valgrind \
    zlib1g-dev \
    cmake libclang-dev clang

COPY Cargo.toml Cargo.lock  ./
COPY src  ./src

RUN cargo build --release

RUN apt-get install -y ca-certificates

# final image
FROM debian:buster-slim
COPY --from=build-env /app/target/release/near-event-streams /app/
COPY --from=build-env /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
WORKDIR /app

ENTRYPOINT ["./near-event-streams"]
