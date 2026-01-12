# Repository Guidelines

## Project Structure & Module Organization
- `pkg/` contains public packages: `repository` (main API), `importer`, `extractor`, `validator`, and shared utilities in `helper`.
- `internal/storage/` holds storage backends and configuration (FlatFS/LevelDB, mounting, locking).
- Tests live alongside code as `*_test.go` and include unit, integration, edge-case, and performance coverage.
- `docs/` stores design notes, refactoring plans, and performance analyses.

## Build, Test, and Development Commands
- `go build ./...` builds all packages.
- `go test ./...` runs the full test suite.
- `go test -v ./...` runs tests with verbose output.
- `go test ./... -short` runs short tests (skips long-running cases).
- `go test -race ./...` runs the race detector for concurrency checks.
- `go test ./pkg/importer` (or any package path) runs focused tests.

## Coding Style & Naming Conventions
- Go standard formatting: run `gofmt` on modified files.
- Use idiomatic Go naming (exported `CamelCase`, unexported `camelCase`, package names lowercase).
- Prefer small, focused helpers and avoid introducing new global state.

## Testing Guidelines
- Tests follow Go conventions: `*_test.go`, `TestXxx`, `BenchmarkXxx` for benchmarks.
- Keep table-driven tests when multiple cases are expected.
- When adding features or fixes, include unit tests and update integration tests if behavior changes.
- Storage lock tests expect blocking behavior on concurrent opens; avoid assuming immediate lock errors.

## Commit & Pull Request Guidelines
- Commit messages follow Conventional Commits seen in history: `feat:`, `fix:`, `perf:`, `refactor:`, `build:`, `chore:`.
- PRs should include a concise summary, testing evidence (command + result), and linked issues when applicable.
- For behavior changes, add a short note about data compatibility or migration impact.

## Architecture Notes
- This repository implements an IPFS-style content-addressed storage system.
- Importer builds a DAG from source files and returns a root CID; extractor reconstructs files from that CID.
- Storage defaults to FlatFS for blocks and LevelDB for metadata; config is validated on startup.
