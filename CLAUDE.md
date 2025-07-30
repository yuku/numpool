# CLAUDE.md

## Test Commands

- Run tests: `go test -v ./...`
- Run formatter: `go fmt ./...`
- Run linter: `golangci-lint run`
- Generate SQL queries: `make`

## Development Principles

- **Pure Functions**: Implement functionality as pure functions whenever possible
- **Go Best Practices**: Follow standard Go best practices and idioms
- **Documentation**: Write comments for all exported functions, structs, and other exported types
- **Test-Driven Development (TDD)**:
  - First, write a failing test. Confirm that the test fails as intended, then commit.
  - Next, implement code to make the test pass. Confirm the test passes, then commit.
  - Note: Developers may write tests. In such cases, Claude Code's job is to implement code that makes the developer's tests pass.
  - **IMPORTANT**: When a failing test exists, ALWAYS commit it before starting implementation
  - **IMPORTANT**: After implementing code, ALWAYS run the test to confirm it passes before committing

## Platform Support

- Support Linux only. No need to support Windows.
