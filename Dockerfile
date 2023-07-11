FROM rust:1.69.0-buster as builder

WORKDIR /usr/src/joie

COPY Cargo.toml . 
COPY Cargo.lock .

COPY storage storage
COPY engine engine


RUN mkdir satt
COPY satt/src satt/src
COPY satt/Cargo.toml satt/Cargo.toml

RUN cargo build --profile production --bin satt

FROM debian:buster-slim

RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get install wget -y

WORKDIR /joie
RUN chown -R 1000:1000 /joie

USER 1000

RUN mkdir run
WORKDIR run
COPY --from=builder /usr/src/joie/target/production/satt .
COPY satt/static static

CMD ["./satt"]