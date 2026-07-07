// Package server implements the FrenzyPG proxy server.
//
// File organization:
//   - proxy.go          — Core proxy server: struct, config, lifecycle, wire protocol parsing
//   - query_executor.go — Query execution: primary/mirror execution, parameter conversion
//   - mirror.go         — Mirror routing: query filtering, transaction buffering, async dispatch
//   - connection.go     — Database connections: pool management, SSL, query forwarding
//   - connection_affinity.go — Connection affinity for transaction session pinning
//   - session_manager.go    — Client session state and transaction tracking
//   - session_id.go         — Stable session ID generation via psql-wire session
//   - transaction_aware_connection.go — Transaction-aware connection wrapper
//   - simple_transaction_fix.go       — Global transaction state tracker
//   - tls_listener.go       — TLS listener with PostgreSQL SSL negotiation
package server
