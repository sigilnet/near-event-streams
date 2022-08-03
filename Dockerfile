FROM rust:1.61 as base-env

FROM base-env AS build-env

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential \
    curl \
    openssl libssl-dev \
    pkg-config \
    python \
    valgrind \
    zlib1g-dev \
    cmake libclang-dev

COPY Cargo.toml Cargo.lock  ./
COPY src  ./src

RUN cargo build --release

# final image
FROM base-env
COPY --from=build-env /app/target/release/near-event-streams /app/
WORKDIR /app

ENTRYPOINT ["./near-event-streams"]
