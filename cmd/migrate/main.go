package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/metabrainz/synapse/internal/config"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	direction := flag.Arg(0)
	if direction != "up" && direction != "down" && direction != "force" {
		fmt.Fprintln(os.Stderr, "usage: migrate [-config path] <up|down|force <version>>")
		os.Exit(1)
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	m, err := migrate.New("file://migrations", cfg.Postgres.URL())
	if err != nil {
		slog.Error("init migrate", "err", err)
		os.Exit(1)
	}
	defer m.Close()

	switch direction {
	case "up":
		err = m.Up()
	case "down":
		err = m.Steps(-1)
	case "force":
		versionStr := flag.Arg(1)
		if versionStr == "" {
			fmt.Fprintln(os.Stderr, "usage: migrate force <version>")
			os.Exit(1)
		}
		var version int
		if _, scanErr := fmt.Sscanf(versionStr, "%d", &version); scanErr != nil {
			fmt.Fprintf(os.Stderr, "force: invalid version %q\n", versionStr)
			os.Exit(1)
		}
		err = m.Force(version)
	}

	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		slog.Error("migrate", "direction", direction, "err", err)
		os.Exit(1)
	}

	slog.Info("migrate done", "direction", direction)
}
