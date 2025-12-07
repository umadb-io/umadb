SHELL := /bin/bash

PYTHONPATH=./tests

UV_VERSION=0.9.16
UV ?= uv@$(UV_VERSION)
UVX ?= uvx@$(UV_VERSION)

.PHONY: clean
.PHONY: build
.PHONY: build-workspace-exclude-python
.PHONY: test
.PHONY: test-workspace-exclude-python
.PHONY: bench-append bench-append-1 bench-append-10 bench-append-100 bench-append-1000 bench-append-all
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
.PHONY: cross_build_umadb
.PHONY: cross_build_throughput_vs_volume
.PHONY: build-cross-throughput-vs-volume-x86_64-unknown-linux-musl
.PHONY: build-cross-throughput-vs-volume-aarch64-unknown-linux-musl
.PHONY: test-cross-umadb-all
.PHONY: test-cross-umadb-x86_64-unknown-linux-musl
.PHONY: test-cross-umadb-aarch64-unknown-linux-musl
.PHONY: test-cross-umadb-x86_64-unknown-linux-gnu
.PHONY: test-cross-umadb-aarch64-unknown-linux-gnu
.PHONY: test-cross-umadb-x86_64-apple-darwin
.PHONY: test-cross-umadb-aarch64-apple-darwin
.PHONY: install-uv
.PHONY: install-python-tools
.PHONY: install-python-packages
.PHONY: update-python-packages
.PHONY: build-umadb-python
.PHONY: maturin-python-stubs
.PHONY: maturin-python-develop
.PHONY: maturin-python-build
.PHONY: maturin-python-build-release

clean:
	cargo clean

build:
	$(MAKE) build-workspace-exclude-python
	$(MAKE) build-umadb-python

build-workspace-exclude-python:
	cargo build --workspace --exclude umadb-python

test:
	$(MAKE) test-workspace-exclude-python
	$(MAKE) test-umadb-python

test-workspace-exclude-python:
	cargo test --workspace --exclude umadb-python


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

bench-append-1000:
	$(MAKE) bench-append EVENTS_PER_REQUEST=1000

bench-append-all:
	$(MAKE) bench-append-1
	$(MAKE) bench-append-10
	$(MAKE) bench-append-100

bench-append:
	@echo "Running benchmark with EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST)"
	@trap 'kill 0' INT TERM; \
	EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST) MAX_THREADS=$(MAX_THREADS) cargo bench -p umadb-benches --bench grpc_append_bench && \
	EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST) MAX_THREADS=$(MAX_THREADS) python ./umadb-benches/benches/grpc_append_bench_plot.py

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
	EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST) MAX_THREADS=$(MAX_THREADS) python ./umadb-benches/benches/grpc_append_cond_bench_plot.py

bench-append-with-readers:
	@echo "Running append with readers benchmark"
	@trap 'kill 0' INT TERM; \
	MAX_THREADS=$(MAX_THREADS) cargo bench -p umadb-benches --bench grpc_append_with_readers_bench && \
	python ./umadb-benches/benches/grpc_append_with_readers_bench_plot.py

bench-read:
	@echo "Running read benchmark"
	@trap 'kill 0' INT TERM; \
	MAX_THREADS=$(MAX_THREADS) BENCH_READ_THROTTLED=$(BENCH_READ_THROTTLED) cargo bench -p umadb-benches --bench grpc_read_bench && \
	MAX_THREADS=$(MAX_THREADS) BENCH_READ_THROTTLED=$(BENCH_READ_THROTTLED) python ./umadb-benches/benches/grpc_read_bench_plot.py

bench-read-throttled:
	$(MAKE) bench-read BENCH_READ_THROTTLED=1

bench-read-cond:
	@echo "Running conditional read benchmark"
	@trap 'kill 0' INT TERM; \
	MAX_THREADS=$(MAX_THREADS) cargo bench -p umadb-benches --bench grpc_read_cond_bench && \
	MAX_THREADS=$(MAX_THREADS) python ./umadb-benches/benches/grpc_read_cond_bench_plot.py

bench-read-with-writers:
	@echo "Running read with writers benchmark"
	@trap 'kill 0' INT TERM; \
	MAX_THREADS=$(MAX_THREADS) cargo bench -p umadb-benches --bench grpc_read_with_writers_bench && \
	MAX_THREADS=$(MAX_THREADS) python ./umadb-benches/benches/grpc_read_with_writers_bench_plot.py

bench-throughput-vs-volume:
	@trap 'kill 0' INT TERM; \
	cargo bench -p umadb-benches --bench throughput_vs_volume


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
ensure_cross:
	@if [ -z "$(CROSS)" ]; then \
		echo "🔧 Installing cross..."; \
		cargo install cross --git https://github.com/cross-rs/cross || exit 1; \
	else \
		echo "🔧 Found cross..."; \
	fi

# Install rust target
ensure_target:
	@echo "🔧 Ensuring Rust target: $(RUST_TARGET)"
	rustup target add $(RUST_TARGET)


# Helper to build with platform if needed
cross_build_umadb:
	$(MAKE) ensure_cross
	$(MAKE) ensure_target
	TARGET_ARCH=`echo "$(RUST_TARGET)" | cut -d"-" -f1`; \
	TARGET_OS=`echo "$(RUST_TARGET)" | cut -d"-" -f3`; \
	TARGET_ABI=`echo "$(RUST_TARGET)" | cut -d"-" -f4`; \
	if [ "$(HOST_ARCH)" = "arm64" ]; then HOST_ARCH=aarch64; else HOST_ARCH=$(HOST_ARCH); fi; \
	echo "🚀 Building RUST_TARGET: $$RUST_TARGET"; \
	echo "BIN_PATH: $$BIN_PATH"; \
	echo "Target Arch: $$TARGET_ARCH"; \
	echo "Target OS: $$TARGET_OS"; \
	echo "Host Arch: $$HOST_ARCH"; \
	echo "Host OS: $$HOST_OS"; \
	if [ $$HOST_OS = "Darwin" ] && [ $$TARGET_OS == "darwin" ]; then \
		echo "🏃 Running build with cargo directly on macOS host..."; \
      	CARGO_BUILD_BUILD_DIR="target/$(RUST_TARGET)/build" cargo build --release --target "$(RUST_TARGET)" --package umadb; \
	else \
	  	echo "🐳 Running build with cross on Docker"; \
      	CARGO_BUILD_BUILD_DIR="target/$(RUST_TARGET)/build" cross build --release --target "$(RUST_TARGET)" --package umadb; \
	fi

# Helper to build with platform if needed
cross_build_throughput_vs_volume:
	$(MAKE) ensure_cross
	$(MAKE) ensure_target
	TARGET_ARCH=`echo "$(RUST_TARGET)" | cut -d"-" -f1`; \
	TARGET_OS=`echo "$(RUST_TARGET)" | cut -d"-" -f3`; \
	TARGET_ABI=`echo "$(RUST_TARGET)" | cut -d"-" -f4`; \
	if [ "$(HOST_ARCH)" = "arm64" ]; then HOST_ARCH=aarch64; else HOST_ARCH=$(HOST_ARCH); fi; \
	echo "🚀 Building RUST_TARGET: $$RUST_TARGET"; \
	echo "BIN_PATH: $$BIN_PATH"; \
	echo "Target Arch: $$TARGET_ARCH"; \
	echo "Target OS: $$TARGET_OS"; \
	echo "Host Arch: $$HOST_ARCH"; \
	echo "Host OS: $$HOST_OS"; \
	if [ $$HOST_OS = "Darwin" ] && [ $$TARGET_OS == "darwin" ]; then \
		echo "🏃 Running build with cargo directly on macOS host..."; \
      	CARGO_BUILD_BUILD_DIR="target/$(RUST_TARGET)/build" cargo build --release --target "$(RUST_TARGET)" --package umadb-benches; \
	else \
	  	echo "🐳 Running build with cross on Docker"; \
      	CARGO_BUILD_BUILD_DIR="target/$(RUST_TARGET)/build" cross build --release --target "$(RUST_TARGET)" --package umadb-benches; \
	fi


# ---------------------------------------------
# Build targets
# ---------------------------------------------

build-cross-umadb-x86_64-unknown-linux-musl:
	$(MAKE) cross_build_umadb RUST_TARGET=x86_64-unknown-linux-musl

build-cross-umadb-aarch64-unknown-linux-musl:
	$(MAKE) cross_build_umadb RUST_TARGET=aarch64-unknown-linux-musl

build-cross-umadb-x86_64-unknown-linux-gnu:
	$(MAKE) cross_build_umadb RUST_TARGET=x86_64-unknown-linux-gnu

build-cross-umadb-aarch64-unknown-linux-gnu:
	$(MAKE) cross_build_umadb RUST_TARGET=aarch64-unknown-linux-gnu

build-cross-umadb-x86_64-apple-darwin:
	$(MAKE) cross_build_umadb RUST_TARGET=x86_64-apple-darwin

build-cross-umadb-aarch64-apple-darwin:
	$(MAKE) cross_build_umadb RUST_TARGET=aarch64-apple-darwin

build-cross-umadb-all: \
	build-cross-umadb-x86_64-unknown-linux-musl \
	build-cross-umadb-aarch64-unknown-linux-musl \
	build-cross-umadb-x86_64-unknown-linux-gnu \
	build-cross-umadb-aarch64-unknown-linux-gnu \
	build-cross-umadb-x86_64-apple-darwin \
	build-cross-umadb-aarch64-apple-darwin

build-cross-throughput-vs-volume-x86_64-unknown-linux-musl:
	$(MAKE) cross_build_throughput_vs_volume RUST_TARGET=x86_64-unknown-linux-musl

build-cross-throughput-vs-volume-aarch64-unknown-linux-musl:
	$(MAKE) cross_build_throughput_vs_volume RUST_TARGET=aarch64-unknown-linux-musl


# ---------------------------------------------
# Test targets
# ---------------------------------------------

# Detect host architecture
export HOST_ARCH := $(shell uname -m)

# Detect host OS
export HOST_OS := $(shell uname -s)


run_binary:
	RUST_TARGET=$(RUST_TARGET); \
	BIN_PATH="target/$$RUST_TARGET/release/umadb"; \
	TARGET_ARCH=`echo "$(RUST_TARGET)" | cut -d"-" -f1`; \
	TARGET_OS=`echo "$(RUST_TARGET)" | cut -d"-" -f3`; \
	TARGET_ABI=`echo "$(RUST_TARGET)" | cut -d"-" -f4`; \
	if [ "$(HOST_ARCH)" = "arm64" ]; then HOST_ARCH=aarch64; else HOST_ARCH=$(HOST_ARCH); fi; \
	echo "Rust target: $$RUST_TARGET"; \
	echo "BIN_PATH: $$BIN_PATH"; \
	echo "Target Arch: $$TARGET_ARCH"; \
	echo "Target OS: $$TARGET_OS"; \
	echo "Host Arch: $$HOST_ARCH"; \
	echo "Host OS: $$HOST_OS"; \
	if [ $$HOST_OS = "Darwin" ] && [ $$TARGET_OS = "darwin" ] && [ $$TARGET_ARCH = $$HOST_ARCH ]; then \
		echo "🏃 Running $$BIN_PATH directly on macOS host..."; \
		"$$BIN_PATH" --version; \
	elif [ $$HOST_OS = "Darwin" ] && [ $$TARGET_OS = "darwin" ] && [ $$TARGET_ARCH = "x86_64" ]; then \
		echo "🏃 Running $$BIN_PATH directly on macOS host with Rosetta..."; \
		arch -x86_64 $$BIN_PATH --version; \
	elif [ $$HOST_OS = "Linux" ] && [ $$TARGET_OS = "linux" ] && [ $$TARGET_ARCH = $$HOST_ARCH ]; then \
		echo "🏃 Running $$BIN_PATH directly on Linux host..."; \
		"$$BIN_PATH" --version; \
	elif [ $$TARGET_OS = "linux" ]; then \
		if [ $$TARGET_ARCH = "aarch64" ]; then PLATFORM=linux/arm64; else PLATFORM=linux/amd64; fi; \
		if [ $$TARGET_ABI = "musl" ]; then IMAGE=alpine:3.18; else IMAGE=debian:stable-slim; fi; \
		echo "🐳 Running $$BIN_PATH in Docker, platform: $$PLATFORM, image: $$IMAGE"; \
		docker run --rm --platform $$PLATFORM -v $$(pwd)/$$(dirname "$$BIN_PATH"):/usr/local/bin $$IMAGE sh -c "/usr/local/bin/umadb --version"; \
	else \
		echo "❌ Cannot run $$BIN_PATH on this host or via Docker"; \
		exit 1; \
	fi


# Linux x86_64 MUSL
test-cross-umadb-x86_64-unknown-linux-musl:
	$(MAKE) run_binary RUST_TARGET=x86_64-unknown-linux-musl

# Linux aarch64 MUSL
test-cross-umadb-aarch64-unknown-linux-musl:
	$(MAKE) run_binary RUST_TARGET=aarch64-unknown-linux-musl

# Linux x86_64 GNU
test-cross-umadb-x86_64-unknown-linux-gnu:
	$(MAKE) run_binary RUST_TARGET=x86_64-unknown-linux-gnu

# Linux aarch64 GNU
test-cross-umadb-aarch64-unknown-linux-gnu:
	$(MAKE) run_binary RUST_TARGET=aarch64-unknown-linux-gnu

# macOS x86_64
test-cross-umadb-x86_64-apple-darwin:
	$(MAKE) run_binary RUST_TARGET=x86_64-apple-darwin

# macOS aarch64
test-cross-umadb-aarch64-apple-darwin:
	$(MAKE) run_binary RUST_TARGET=aarch64-apple-darwin

# Run all tests
test-cross-umadb-all: test-cross-umadb-x86_64-unknown-linux-musl \
          test-cross-umadb-aarch64-unknown-linux-musl \
          test-cross-umadb-x86_64-unknown-linux-gnu \
          test-cross-umadb-aarch64-unknown-linux-gnu \
          test-cross-umadb-x86_64-apple-darwin \
          test-cross-umadb-aarch64-apple-darwin
	@echo "✅ All binaries tested!"

install-uv:
	@pipx install --suffix="@$(UV_VERSION)" "uv==$(UV_VERSION)"
	$(UV) --version

install-python-tools:
	$(UV) sync

build-umadb-python: maturin-python-stubs maturin-python-build

maturin-python-stubs:
	$(MAKE) maturin-python-develop
	$(UV) run python ./umadb-python/generate_stubs.py
	$(UV) run mypy --strict ./umadb-python --exclude generate_stubs.py

maturin-python-develop:
	PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 $(UV) run maturin develop --uv -m umadb-python/Cargo.toml

maturin-python-build:
	PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 $(UV) run maturin build -m umadb-python/Cargo.toml

maturin-python-build-release:
	PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 $(UV) run maturin build --release -m umadb-python/Cargo.toml

test-umadb-python:
	{ \
	  cargo build --bin umadb; \
	  cargo run --bin umadb -- --db-path=./uma-tmp.db & \
	  my_process_id=$$!; \
	  echo "PID: $$my_process_id"; \
	  sleep 1; \
	  $(UV) run python ./umadb-python/examples/basic_usage.py; \
	  echo "PID: $$my_process_id"; \
	  kill -SIGINT $$my_process_id; \
	  sleep 1; \
	  rm ./uma-tmp.db;\
	}