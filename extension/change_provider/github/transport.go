package github

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"gopkg.in/yaml.v3"
)

// AppConfig holds GitHub App configuration for authentication.
type AppConfig struct {
	// AppID is the GitHub App ID.
	AppID int64 `yaml:"app_id"`

	// InstallationID is the GitHub App installation ID for the target repo/org.
	InstallationID int64 `yaml:"installation_id"`

	// Owner is the repository owner (user or organization).
	Owner string `yaml:"owner"`

	// Repo is the repository name.
	Repo string `yaml:"repo"`

	// PrivateKey is the PEM-encoded private key contents.
	PrivateKey string `yaml:"private_key"`
}

// LoadConfigFromFile loads AppConfig from a YAML file.
func LoadConfigFromFile(path string) (AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return AppConfig{}, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var config AppConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return AppConfig{}, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	return config, nil
}

// NewHTTPClientFromConfig creates an HTTP client configured with GitHub App authentication.
// The client automatically handles JWT generation and installation token refresh.
func NewHTTPClientFromConfig(config AppConfig) (*http.Client, error) {
	if config.PrivateKey == "" {
		return nil, fmt.Errorf("private_key is required")
	}

	privateKey, err := parsePrivateKey([]byte(config.PrivateKey))
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	transport := &appTransport{
		appID:          config.AppID,
		installationID: config.InstallationID,
		privateKey:     privateKey,
		baseURL:        defaultBaseURL,
		base:           http.DefaultTransport,
	}

	return &http.Client{Transport: transport}, nil
}

// appTransport is an http.RoundTripper that authenticates requests using GitHub App installation tokens.
type appTransport struct {
	appID          int64
	installationID int64
	privateKey     *rsa.PrivateKey
	baseURL        string
	base           http.RoundTripper

	mu    sync.Mutex
	token string
	exp   time.Time
}

// RoundTrip implements http.RoundTripper.
func (t *appTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := t.getToken()
	if err != nil {
		return nil, fmt.Errorf("failed to get installation token: %w", err)
	}

	req2 := req.Clone(req.Context())
	req2.Header.Set("Authorization", "Bearer "+token)

	return t.base.RoundTrip(req2)
}

// getToken returns a valid installation access token, refreshing if needed.
func (t *appTransport) getToken() (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.token != "" && time.Now().Before(t.exp.Add(-time.Minute)) {
		return t.token, nil
	}

	token, exp, err := t.refreshToken()
	if err != nil {
		return "", err
	}

	t.token = token
	t.exp = exp
	return t.token, nil
}

// refreshToken exchanges a JWT for an installation access token.
func (t *appTransport) refreshToken() (string, time.Time, error) {
	jwtToken, err := t.generateJWT()
	if err != nil {
		return "", time.Time{}, err
	}

	url := fmt.Sprintf("%s/app/installations/%d/access_tokens", t.baseURL, t.installationID)
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to create token request: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+jwtToken)

	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to request installation token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return "", time.Time{}, fmt.Errorf("installation token request failed with status %d: %s", resp.StatusCode, body)
	}

	var tokenResp struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", time.Time{}, fmt.Errorf("failed to decode token response: %w", err)
	}

	return tokenResp.Token, tokenResp.ExpiresAt, nil
}

// generateJWT creates a signed JWT for GitHub App authentication.
func (t *appTransport) generateJWT() (string, error) {
	now := time.Now()
	claims := jwt.RegisteredClaims{
		IssuedAt:  jwt.NewNumericDate(now.Add(-time.Minute)),
		ExpiresAt: jwt.NewNumericDate(now.Add(10 * time.Minute)),
		Issuer:    fmt.Sprintf("%d", t.appID),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	return token.SignedString(t.privateKey)
}

// parsePrivateKey parses a PEM-encoded RSA private key.
func parsePrivateKey(pemData []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}

	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private key is not RSA")
	}

	return rsaKey, nil
}
