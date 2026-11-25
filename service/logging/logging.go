// Package logging builds the zap logger shared by the runnable servers.
package logging

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New returns a production logger configured from LOG_LEVEL (debug, info, warn
// or error; default info) and LOG_FORMAT (json or console; default json).
func New() (*zap.Logger, error) {
	cfg, err := configFromEnv(os.Getenv)
	if err != nil {
		return nil, err
	}
	return cfg.Build()
}

func configFromEnv(getenv func(string) string) (zap.Config, error) {
	cfg := zap.NewProductionConfig()

	if v := getenv("LOG_LEVEL"); v != "" {
		var level zap.AtomicLevel
		if err := level.UnmarshalText([]byte(v)); err != nil || !allowedLevel(v) {
			return zap.Config{}, fmt.Errorf("invalid LOG_LEVEL %q: want debug, info, warn or error", v)
		}
		cfg.Level = level
	}

	switch v := getenv("LOG_FORMAT"); v {
	case "", "json":
	case "console":
		cfg.Encoding = "console"
		cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	default:
		return zap.Config{}, fmt.Errorf("invalid LOG_FORMAT %q: want json or console", v)
	}
	return cfg, nil
}

func allowedLevel(v string) bool {
	switch v {
	case "debug", "info", "warn", "error":
		return true
	}
	return false
}
