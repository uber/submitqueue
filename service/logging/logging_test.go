package logging

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestConfigFromEnv(t *testing.T) {
	tests := []struct {
		name         string
		env          map[string]string
		wantErr      bool
		wantDebug    bool
		wantInfo     bool
		wantEncoding string
	}{
		{name: "empty env", wantInfo: true, wantEncoding: "json"},
		{name: "debug level", env: map[string]string{"LOG_LEVEL": "debug"}, wantDebug: true, wantInfo: true, wantEncoding: "json"},
		{name: "warn level", env: map[string]string{"LOG_LEVEL": "warn"}, wantEncoding: "json"},
		{name: "invalid level", env: map[string]string{"LOG_LEVEL": "verbose"}, wantErr: true},
		{name: "invalid format", env: map[string]string{"LOG_FORMAT": "xml"}, wantErr: true},
		{name: "console format", env: map[string]string{"LOG_FORMAT": "console"}, wantInfo: true, wantEncoding: "console"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := configFromEnv(func(k string) string { return tt.env[k] })
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantDebug, cfg.Level.Enabled(zap.DebugLevel))
			assert.Equal(t, tt.wantInfo, cfg.Level.Enabled(zap.InfoLevel))
			assert.Equal(t, tt.wantEncoding, cfg.Encoding)
		})
	}
}
