# syntax=docker/dockerfile:1.7
#
# doewood: one binary in one image (ADR 0001, section 1), built with OCR.
#
#   docker build -t doewood:dev .
#   docker run --rm -p 7443:7443 -p 7444:7444 -v doewood-data:/data doewood:dev
#
# See docs/deploy.md.

# The Rust version of rust-toolchain.toml; the build checks that they match.
ARG RUST_VERSION=1.97.1
ARG DEBIAN_RELEASE=trixie

# --- build ----------------------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-slim-${DEBIAN_RELEASE} AS build
# The release profile uses fat LTO: lower this if the build runs out of memory.
ARG CARGO_BUILD_JOBS=4
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS} \
    CARGO_TERM_COLOR=never \
    # The toolchain of the image, without the clippy and rustfmt components the toolchain file
    # asks for (only development needs them).
    RUSTUP_TOOLCHAIN=${RUST_VERSION}
WORKDIR /src
COPY rust-toolchain.toml ./
RUN want="$(sed -n 's/^channel = "\(.*\)"$/\1/p' rust-toolchain.toml)"; \
    have="$(rustc --version | cut -d' ' -f2)"; \
    [ "$want" = "$have" ] || { echo "rust-toolchain.toml says $want, the image has $have" >&2; exit 1; }
COPY Cargo.toml Cargo.lock ./
COPY proto proto
COPY crates crates
# Cache mounts keep the registry, the git checkouts, ONNX Runtime (downloaded by the `ort` build
# script) and the target directory between local builds. The binary is copied out of the cached
# target directory in the same step.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/root/.cache/ort.pyke.io \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p doewood \
 && install -D -m 0755 target/release/doewood /out/doewood

# --- OCR models -----------------------------------------------------------------------------------
# quail's OCR (PP-OCRv6 on ONNX Runtime, statically linked into the binary) needs these 7 files.
# URLs and SHA-256 are those of `quail_ocr::models::MANIFEST` at the quail revision in Cargo.lock;
# update them together. Apache-2.0 weights, about 45 MiB.
FROM scratch AS models
ARG MODELS=https://www.modelscope.cn/api/v1/models/greatv/oar-ocr/repo?Revision=master&FilePath=
ADD --chmod=0444 --checksum=sha256:d73e0058b7a8086bbd57f3d10b8bcd4ff95363f67e06e2762b5e814fe9c9410e ${MODELS}pp-ocrv6_small_det.onnx /models/pp-ocrv6_small_det.onnx
ADD --chmod=0444 --checksum=sha256:5435fd747c9e0efe15a96d0b378d5bd157e9492ed8fd80edf08f30d02fa24634 ${MODELS}pp-ocrv6_small_rec.onnx /models/pp-ocrv6_small_rec.onnx
ADD --chmod=0444 --checksum=sha256:b5f2bfe2bdd9448429e3e82b51c789775d9b42f2403d082b00662eb77e401c5d ${MODELS}ppocrv6_dict.txt /models/ppocrv6_dict.txt
ADD --chmod=0444 --checksum=sha256:a18d96d7c8d73d90f2ed056549caa1de3a8e6cb744cccba16cd593ea8cd2d569 ${MODELS}cyrillic_pp-ocrv5_mobile_rec.onnx /models/cyrillic_pp-ocrv5_mobile_rec.onnx
ADD --chmod=0444 --checksum=sha256:db40aa52ceb112055be80c694afdf655d5d2c4f7873704524cc16a447ca913ba ${MODELS}ppocrv5_cyrillic_dict.txt /models/ppocrv5_cyrillic_dict.txt
ADD --chmod=0444 --checksum=sha256:bbcd6c2b43ab15d2e605455aef2cd280ab87b570824522d24a69e9298875a1ac ${MODELS}pp-lcnet_x1_0_doc_ori.onnx /models/pp-lcnet_x1_0_doc_ori.onnx
ADD --chmod=0444 --checksum=sha256:fb402220f39b183d64a68cd48d4bd53267a21354b6fc39370c2a83fbdad85b10 ${MODELS}pp-lcnet_x0_25_textline_ori.onnx /models/pp-lcnet_x0_25_textline_ori.onnx

# --- runtime --------------------------------------------------------------------------------------
FROM debian:${DEBIAN_RELEASE}-slim
# CA roots for S3 and Postgres TLS; libstdc++ for the statically linked ONNX Runtime.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 doewood \
 && useradd --system --uid 10001 --gid 10001 --home-dir /data --no-create-home --shell /usr/sbin/nologin doewood \
 && install -d -o 10001 -g 10001 -m 0750 /data
COPY --from=build /out/doewood /usr/local/bin/doewood
COPY --from=models /models /opt/doewood/ocr-models
# Bootstrap variables (docs/configuration.md): listen on every interface inside the container,
# keep the store on the volume, use the models of the image.
ENV DOEWOOD_DATA_DIR=/data \
    DOEWOOD_LISTEN=0.0.0.0:7443 \
    DOEWOOD_OPS_LISTEN=0.0.0.0:7444 \
    DOEWOOD_OCR_MODELS=/opt/doewood/ocr-models
VOLUME /data
EXPOSE 7443 7444
USER 10001:10001
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/doewood"]
CMD ["serve"]
