internal/sqlc/query.sql.go: internal/sqlc/query.sql internal/sqlc/schema.sql
	@go tool sqlc generate -f  internal/sqlc/sqlc.yaml
