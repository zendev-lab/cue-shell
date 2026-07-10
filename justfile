# List all available commands
default:
    @just --list

# Format all code
format:
    just --fmt --unstable
    cargo fmt

# Run all static checks (fmt check + clippy)
check:
    cargo fmt --all -- --check
    cargo clippy --all-targets -- -D warnings

# Run tests
test *ARGS:
    cargo test {{ARGS}}

# Exercise cue-tui's first-party debug socket in a real PTY
tui-debug-smoke:
    uv run scripts/cue_tui_debug_smoke.py

# Run tests with coverage (requires cargo-llvm-cov)
cov:
    cargo llvm-cov test --lcov --output-path lcov.info -- --no-capture

# Open coverage HTML report
cov-open:
    cargo llvm-cov test --html -- --no-capture
    open target/llvm-cov/html/index.html || xdg-open target/llvm-cov/html/index.html

# MSRV check. Cargo enforces workspace.package.rust-version (1.95), so this
# works with both rustup-managed and standalone/Homebrew toolchains.
msrv:
    cargo check --workspace --all-targets

# Clean build artifacts
clean:
    rm -rf target/
    rm -f lcov.info

# Full CI check (format check + clippy + test + MSRV)
ci: check test msrv

# Run pre-commit on all files
pre-commit:
    uvx prek run --all-files

# Install local git hooks via prek
pre-commit-install:
    uvx prek install --install-hooks --hook-type pre-commit --hook-type commit-msg

# Remove local git hooks installed by prek
pre-commit-uninstall:
    uvx prek uninstall

# Display project information
info:
    @echo "=== cue-shell ==="
    @echo "Rust: $(rustc --version)"
    @echo "Cargo: $(cargo --version)"
    @echo ""
    @echo "Workspace members:"
    @cargo metadata --no-deps --format-version 1 2>/dev/null | jq -r '.packages[].name' 2>/dev/null || echo "  (install jq for detailed info)"
