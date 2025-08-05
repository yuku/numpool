internal/sqlc/query.sql.go: internal/sqlc/query.sql internal/sqlc/schema.sql
	@go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.29.0 generate -f  internal/sqlc/sqlc.yaml
