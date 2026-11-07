package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/juantevez/go-posnet/context/terminal-gateway/config"
)

func TestTerminalTokens(t *testing.T) {
	cfgFor := func(environment, path string) *config.Config {
		cfg := &config.Config{TerminalTokensFile: path}
		cfg.OTEL.Environment = environment
		return cfg
	}

	t.Run("sin archivo en desarrollo, la API queda abierta", func(t *testing.T) {
		tokens, err := terminalTokens(cfgFor("development", ""))
		if err != nil || tokens.Configured() {
			t.Errorf("terminalTokens() = %v, %v, want abierta", tokens, err)
		}
	})

	for _, env := range []string{"production", "staging", ""} {
		t.Run("sin archivo en "+env+", no arranca", func(t *testing.T) {
			if _, err := terminalTokens(cfgFor(env, "")); err == nil {
				t.Error("terminalTokens() error = nil, want error")
			}
		})
	}

	t.Run("con archivo, la API exige token", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "tokens.json")
		if err := os.WriteFile(path, []byte(`[]`), 0o600); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		tokens, err := terminalTokens(cfgFor("production", path))
		if err != nil || !tokens.Configured() {
			t.Errorf("terminalTokens() = %v, %v, want configurado", tokens, err)
		}
	})

	t.Run("archivo inválido, no arranca", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "tokens.json")
		if err := os.WriteFile(path, []byte(`{`), 0o600); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		if _, err := terminalTokens(cfgFor("development", path)); err == nil {
			t.Error("terminalTokens() error = nil, want error")
		}
	})
}
