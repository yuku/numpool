internal/sqlc/query.sql.go: internal/sqlc/query.sql internal/statedb/schema.sql
	@go tool sqlc generate -f  internal/sqlc/sqlc.yaml
