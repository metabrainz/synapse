package config

import (
	"os"
	"testing"
)

func TestLoad_ValidYAML(t *testing.T) {
	f, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	f.WriteString(`
http:
  port: 9090
postgres:
  host: testhost
  port: 5432
  user: testuser
  password: testpass
  dbname: testdb
`)
	f.Close()

	cfg, err := Load(f.Name())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HTTP.Port != 9090 {
		t.Errorf("expected port 9090, got %d", cfg.HTTP.Port)
	}
	if cfg.Postgres.Host != "testhost" {
		t.Errorf("expected host testhost, got %s", cfg.Postgres.Host)
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	f, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	f.WriteString(`
postgres:
  password: fromyaml
`)
	f.Close()

	t.Setenv("SYNAPSE_PG_PASSWORD", "fromenv")

	cfg, err := Load(f.Name())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Postgres.Password != "fromenv" {
		t.Errorf("expected env to override yaml, got %s", cfg.Postgres.Password)
	}
}

func TestLoad_Defaults(t *testing.T) {
	f, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	f.WriteString(`{}`)
	f.Close()

	cfg, err := Load(f.Name())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HTTP.Port != 8080 {
		t.Errorf("expected default port 8080, got %d", cfg.HTTP.Port)
	}
	if cfg.HTTP.DBConns != 20 {
		t.Errorf("expected default api db_conns 20, got %d", cfg.HTTP.DBConns)
	}
	if cfg.Ingest.Workers != 4 {
		t.Errorf("expected default ingest workers 4, got %d", cfg.Ingest.Workers)
	}
	if cfg.Relay.OutboxPollMs != 100 {
		t.Errorf("expected default outbox_poll_ms 100, got %d", cfg.Relay.OutboxPollMs)
	}
}
