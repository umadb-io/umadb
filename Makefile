.PHONY: clean
.PHONY: build
.PHONY: build-workspace-exclude-python
.PHONY: build-umadb-python
.PHONY: bench-append bench-append-1 bench-append-10 bench-append-100 bench-append-all
.PHONY: bench-append-cond bench-append-cond-1 bench-append-cond-10 bench-append-cond-100 bench-append-cond-all
.PHONY: bench-append-with-readers
.PHONY: bench-read bench-read-throttled
.PHONY: bench-read-cond
.PHONY: bench-read-with-writers
.PHONY: bench-all
.PHONY: release-dry-run
.PHONY: release-execute
.PHONY: build-cross-umadb-all
.PHONY: build-cross-umadb-x86_64-unknown-linux-musl
.PHONY: build-cross-umadb-aarch64-unknown-linux-musl
.PHONY: build-cross-umadb-x86_64-unknown-linux-gnu
.PHONY: build-cross-umadb-aarch64-unknown-linux-gnu
.PHONY: build-cross-umadb-x86_64-apple-darwin
.PHONY: build-cross-umadb-aarch64-apple-darwin
.PHONY: test-cross-umadb-all
.PHONY: test-cross-umadb-x86_64-unknown-linux-musl
.PHONY: test-cross-umadb-aarch64-unknown-linux-musl
.PHONY: test-cross-umadb-x86_64-unknown-linux-gnu
.PHONY: test-cross-umadb-aarch64-unknown-linux-gnu
.PHONY: test-cross-umadb-x86_64-apple-darwin
.PHONY: test-cross-umadb-aarch64-apple-darwin

clean:
	cargo clean

build:
	$(MAKE) build-workspace-exclude-python
	$(MAKE) build-umadb-python

build-workspace-exclude-python:
	cargo build --workspace --exclude umadb-python

build-umadb-python:
	PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin develop -m crates/python/Cargo.toml

test:
	$(MAKE) test-workspace-exclude-python
	$(MAKE) test-umadb-python

test-workspace-exclude-python:
	cargo test --workspace --exclude umadb-python

test-umadb-python:
	echo "No Python tests, yet..."


# Default EVENTS_PER_REQUEST to 10 if not provided
EVENTS_PER_REQUEST ?= 10

bench-all:
	$(MAKE) bench-append-all
	$(MAKE) bench-append-cond-all
	$(MAKE) bench-append-with-readers
	$(MAKE) bench-read
	$(MAKE) bench-read-throttled
	$(MAKE) bench-read-cond
	$(MAKE) bench-read-with-writers

bench-append-1:
	$(MAKE) bench-append EVENTS_PER_REQUEST=1

bench-append-10:
	$(MAKE) bench-append EVENTS_PER_REQUEST=10

bench-append-100:
	$(MAKE) bench-append EVENTS_PER_REQUEST=100

bench-append-all:
	$(MAKE) bench-append-1
	$(MAKE) bench-append-10
	$(MAKE) bench-append-100

bench-append:
	@echo "Running benchmark with EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST)"
	@trap 'kill 0' INT TERM; \
	EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST) MAX_THREADS=$(MAX_THREADS) cargo bench -p umadb-benches --bench grpc_append_bench && \
	EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST) MAX_THREADS=$(MAX_THREADS) python ./crates/benches/benches/grpc_append_bench_plot.py

bench-append-cond-all:
	$(MAKE) bench-append-cond-1
	$(MAKE) bench-append-cond-10
	$(MAKE) bench-append-cond-100

bench-append-cond-1:
	$(MAKE) bench-append-cond EVENTS_PER_REQUEST=1

bench-append-cond-10:
	$(MAKE) bench-append-cond EVENTS_PER_REQUEST=10

bench-append-cond-100:
	$(MAKE) bench-append-cond EVENTS_PER_REQUEST=100

bench-append-cond:
	@echo "Running conditional append benchmark with EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST)"
	@trap 'kill 0' INT TERM; \
	EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST) MAX_THREADS=$(MAX_THREADS) cargo bench -p umadb-benches --bench grpc_append_cond_bench && \
	EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST) MAX_THREADS=$(MAX_THREADS) python ./crates/benches/benches/grpc_append_cond_bench_plot.py

bench-append-with-readers:
	@echo "Running append with readers benchmark"
	@trap 'kill 0' INT TERM; \
	MAX_THREADS=$(MAX_THREADS) cargo bench -p umadb-benches --bench grpc_append_with_readers_bench && \
	python ./crates/benches/benches/grpc_append_with_readers_bench_plot.py

bench-read:
	@echo "Running read benchmark"
	@trap 'kill 0' INT TERM; \
	MAX_THREADS=$(MAX_THREADS) BENCH_READ_THROTTLED=$(BENCH_READ_THROTTLED) cargo bench -p umadb-benches --bench grpc_read_bench && \
	MAX_THREADS=$(MAX_THREADS) BENCH_READ_THROTTLED=$(BENCH_READ_THROTTLED) python ./crates/benches/benches/grpc_read_bench_plot.py

bench-read-throttled:
	$(MAKE) bench-read BENCH_READ_THROTTLED=1

bench-read-cond:
	@echo "Running conditional read benchmark"
	@trap 'kill 0' INT TERM; \
	MAX_THREADS=$(MAX_THREADS) cargo bench -p umadb-benches --bench grpc_read_cond_bench && \
	MAX_THREADS=$(MAX_THREADS) python ./crates/benches/benches/grpc_read_cond_bench_plot.py

bench-read-with-writers:
	@echo "Running read with writers benchmark"
	@trap 'kill 0' INT TERM; \
	MAX_THREADS=$(MAX_THREADS) cargo bench -p umadb-benches --bench grpc_read_with_writers_bench && \
	MAX_THREADS=$(MAX_THREADS) python ./crates/benches/benches/grpc_read_with_writers_bench_plot.py


release-crates-dry-run:
	PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo release --workspace

release-crates-execute:
	PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 PUBLISH_GRACE_SLEEP=30 cargo release --workspace --execute


# ---------------------------------------------
# Cross Build Helpers
# ---------------------------------------------

CROSS := $(shell command -v cross 2>/dev/null)
CARGO := cargo

# Detect host architecture
HOST_ARCH := $(shell uname -m)

# Detect host OS
HOST_OS := $(shell uname -s)


# Install cross if needed
define ensure_cross
	@if [ -z "$(CROSS)" ]; then \
		echo "🔧 Installing cross..."; \
		cargo install cross --git https://github.com/cross-rs/cross || exit 1; \
	fi
endef

# Install rust target
define ensure_target
	@echo "🔧 Ensuring Rust target: $(1)"
	rustup target add $(1) >/dev/null 2>&1 || true
endef

# Determine CROSS_PLATFORM automatically for Docker emulation
define platform_for
	$(if $(filter x86_64,$(HOST_ARCH)),$(1),$(if $(filter aarch64,$(HOST_ARCH)),$(2),linux/amd64))
endef

# Helper to build with platform if needed
define cross_build
	$(call ensure_cross)
	$(call ensure_target,$(1))
	@echo "🚀 Building $(1)..."
	@if [ "$(HOST_ARCH)" = "arm64" ] || [ "$(HOST_ARCH)" = "aarch64" ]; then \
		if [ "$(2)" = "x86_64" ]; then \
			CROSS_PLATFORM=linux/amd64 CARGO_BUILD_BUILD_DIR=target/$(1) cross build --release --package umadb --target $(1); \
		else \
			CARGO_BUILD_BUILD_DIR=target/$(1) cross build --release --package umadb --target $(1); \
		fi \
	else \
		CARGO_BUILD_BUILD_DIR=target/$(1) cross build --release --package umadb --target $(1); \
	fi
endef

# ---------------------------------------------
# Build targets
# ---------------------------------------------

build-cross-umadb-x86_64-unknown-linux-musl:
	$(call cross_build,x86_64-unknown-linux-musl,x86_64)

build-cross-umadb-aarch64-unknown-linux-musl:
	$(call cross_build,aarch64-unknown-linux-musl,aarch64)

build-cross-umadb-x86_64-unknown-linux-gnu:
	$(call cross_build,x86_64-unknown-linux-gnu,x86_64)

build-cross-umadb-aarch64-unknown-linux-gnu:
	$(call cross_build,aarch64-unknown-linux-gnu,aarch64)

build-cross-umadb-x86_64-apple-darwin:
	$(call cross_build,x86_64-apple-darwin,x86_64)

build-cross-umadb-aarch64-apple-darwin:
	$(call cross_build,aarch64-apple-darwin,aarch64)

build-cross-umadb-all: \
	build-cross-umadb-x86_64-unknown-linux-musl \
	build-cross-umadb-aarch64-unknown-linux-musl \
	build-cross-umadb-x86_64-unknown-linux-gnu \
	build-cross-umadb-aarch64-unknown-linux-gnu \
	build-cross-umadb-x86_64-apple-darwin \
	build-cross-umadb-aarch64-apple-darwin


# ---------------------------------------------
# Test targets
# ---------------------------------------------

define run_binary
	@bash -c '\
BIN_PATH="$(1)/umadb"; \
TARGET="$(2)"; \
TARGET_ARCH="$(3)"; \
TARGET_OS=$$(echo "$$TARGET" | cut -d"-" -f3); \
HOST_OS=$$(uname -s); \
HOST_ARCH=$$(uname -m); \
if [ "$$HOST_ARCH" = "arm64" ]; then HOST_ARCH=aarch64; fi; \
echo "Host OS: $$HOST_OS, Host Arch: $$HOST_ARCH"; \
echo "Target OS: $$TARGET_OS, Target Arch: $$TARGET_ARCH"; \
\
if [ "$$HOST_OS" = "Darwin" ] && [ "$$TARGET_OS" = "darwin" ] && [ "$$TARGET_ARCH" = "$$HOST_ARCH" ]; then \
    echo "🏃 Running $$BIN_PATH directly on macOS host..."; \
    "$$BIN_PATH" --version; \
elif [ "$$HOST_OS" = "Darwin" ] && [ "$$TARGET_OS" = "darwin" ] && [ "$$TARGET_ARCH" = "x86_64" ] && [ "$$HOST_ARCH" = "aarch64" ]; then \
    echo "🏃 Running $$BIN_PATH on Apple Silicon via Rosetta..."; \
    arch -x86_64 "$$BIN_PATH" --version; \
elif [ "$$HOST_OS" = "Linux" ] && [ "$$TARGET_OS" = "linux" ] && [ "$$TARGET_ARCH" = "$$HOST_ARCH" ]; then \
    echo "🏃 Running $$BIN_PATH directly on Linux host..."; \
    "$$BIN_PATH" --version; \
elif [ "$$TARGET_OS" = "linux" ]; then \
    if [ "$$TARGET_ARCH" = "aarch64" ]; then PLATFORM=linux/arm64; else PLATFORM=linux/amd64; fi; \
    if echo "$$TARGET" | grep -q "musl"; then IMAGE=alpine:3.18; else IMAGE=debian:stable-slim; fi; \
    echo "🐳 Running $$BIN_PATH in Docker, platform: $$PLATFORM, image: $$IMAGE"; \
    docker run --rm --platform $$PLATFORM -v $$(pwd)/$$(dirname "$$BIN_PATH"):/usr/local/bin $$IMAGE sh -c "/usr/local/bin/umadb --version"; \
else \
    echo "❌ Cannot run $$BIN_PATH on this host or via Docker"; \
    exit 1; \
fi'
endef


# Linux x86_64 MUSL
test-cross-umadb-x86_64-unknown-linux-musl:
	$(call run_binary,target/x86_64-unknown-linux-musl/release,x86_64-unknown-linux-musl,x86_64)

# Linux aarch64 MUSL
test-cross-umadb-aarch64-unknown-linux-musl:
	$(call run_binary,target/aarch64-unknown-linux-musl/release,aarch64-unknown-linux-musl,aarch64)

# Linux x86_64 GNU
test-cross-umadb-x86_64-unknown-linux-gnu:
	$(call run_binary,target/x86_64-unknown-linux-gnu/release,x86_64-unknown-linux-gnu,x86_64)

# Linux aarch64 GNU
test-cross-umadb-aarch64-unknown-linux-gnu:
	$(call run_binary,target/aarch64-unknown-linux-gnu/release,aarch64-unknown-linux-gnu,aarch64)

# macOS x86_64
test-cross-umadb-x86_64-apple-darwin:
	$(call run_binary,target/x86_64-apple-darwin/release,x86_64-apple-darwin,x86_64)

# macOS aarch64
test-cross-umadb-aarch64-apple-darwin:
	$(call run_binary,target/aarch64-apple-darwin/release,aarch64-apple-darwin,aarch64)

# Run all tests
test-cross-umadb-all: test-cross-umadb-x86_64-unknown-linux-musl \
          test-cross-umadb-aarch64-unknown-linux-musl \
          test-cross-umadb-x86_64-unknown-linux-gnu \
          test-cross-umadb-aarch64-unknown-linux-gnu \
          test-cross-umadb-x86_64-apple-darwin \
          test-cross-umadb-aarch64-apple-darwin
	@echo "✅ All binaries tested!"
