# zap: binário musl estático numa imagem scratch, sem root.
# Derivado do Dockerfile da raiz; compila o crate apps/zap em vez do demo.
#
# Build (na raiz do repo):  docker build -f apps/zap/Dockerfile -t zap .
# Serviço:  docker run -d -v whatsapp-data:/data zap            (= zap run)
# Consulta: docker exec <container> /zap recent
#
# /data guarda whatsapp.db (sessão) e archive.db (arquivo).

FROM rust:alpine AS chef
RUN apk add --no-cache musl-dev
COPY rust-toolchain.toml .
RUN rustup show && cargo install cargo-chef --locked --version 0.1.77
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY rust-toolchain.toml .
COPY --from=planner /app/recipe.json recipe.json
RUN rustc -vV > /rustc-version \
    && sed -n 's/^host: //p' /rustc-version > /rust-target \
    && test -s /rust-target \
    && rm /rustc-version
RUN cargo chef cook --release --recipe-path recipe.json --target "$(cat /rust-target)" -p zap
COPY . .
RUN cargo build --release -p zap --bin zap --target "$(cat /rust-target)" \
    && cp "target/$(cat /rust-target)/release/zap" /app/zap-bin
RUN mkdir -p /newroot/data /newroot/tmp

FROM scratch
COPY --from=builder --chown=65532:65532 /newroot/tmp /tmp
COPY --from=builder --chown=65532:65532 /newroot/data /data
COPY --from=builder /app/zap-bin /zap
ENV TMPDIR=/tmp
WORKDIR /data
USER 65532:65532
ENTRYPOINT ["/zap"]
CMD ["run"]
