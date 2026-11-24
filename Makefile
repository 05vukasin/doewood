.PHONY: check fmt lint test deny release
check: fmt lint test
fmt:
	cargo fmt --all -- --check
lint:
	cargo clippy --workspace --all-targets -- -D warnings
	cargo clippy -p doewood -p doewood-ingest --no-default-features --all-targets -- -D warnings
test:
	cargo test --workspace
deny:
	cargo deny check
release:
	cargo build --release -p doewood
