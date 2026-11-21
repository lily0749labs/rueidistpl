module github.com/lily0749labs/rueidistpl

go 1.26.0

require (
	github.com/eko/gocache/lib/v4 v4.4.0
	github.com/eko/gocache/store/rueidis/v4 v4.1.14
	github.com/redis/rueidis v1.0.80
	github.com/redis/rueidis/mock v1.0.80
	go.uber.org/mock v0.6.0
)

require (
	github.com/BurntSushi/toml v1.6.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/google/renameio v1.0.1 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pandatix/go-abnf v0.5.5 // indirect
	github.com/prometheus/client_golang v1.25.0 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.74.0 // indirect
	github.com/prometheus/procfs v0.23.0 // indirect
	github.com/redis/rueidis/rueidiscompat v1.0.80 // indirect
	golang.org/x/exp v0.0.0-20261120182113-8801fbdfac81 // indirect
	golang.org/x/exp/typeparams v0.0.0-20261120182113-8801fbdfac81 // indirect
	golang.org/x/mod v0.43.0 // indirect
	golang.org/x/sync v0.25.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/telemetry v0.0.0-20261119160235-508b12de1089 // indirect
	golang.org/x/tools v0.53.0 // indirect
	golang.org/x/vuln v1.10.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	honnef.co/go/tools v0.8.2 // indirect
)

tool (
	golang.org/x/exp/cmd/apidiff
	golang.org/x/vuln/cmd/govulncheck
	honnef.co/go/tools/cmd/staticcheck
)
