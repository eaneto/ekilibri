FROM rust:1-alpine3.18

WORKDIR /usr/src/ekilibri

COPY . .

RUN apk add --no-cache musl-dev python3 py3-pip py3-psutil protoc

RUN rustup target add x86_64-unknown-linux-musl

RUN cargo build --release --target x86_64-unknown-linux-musl

RUN cargo install --path .

RUN python3 -m pip install pytest requests

RUN pytest -s tests --profile=docker
