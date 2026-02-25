package github

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseChangeID(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    ChangeID
		wantErr bool
	}{
		{
			name: "valid github scheme",
			raw:  "github://uber/submitqueue/123/abc123def",
			want: ChangeID{
				Scheme:        "github",
				Org:           "uber",
				Repo:          "submitqueue",
				PRNumber:      123,
				HeadCommitSHA: "abc123def",
			},
		},
		{
			name: "valid ghe scheme",
			raw:  "ghe://uber/monorepo/456/deadbeef",
			want: ChangeID{
				Scheme:        "ghe",
				Org:           "uber",
				Repo:          "monorepo",
				PRNumber:      456,
				HeadCommitSHA: "deadbeef",
			},
		},
		{
			name: "valid ghes scheme",
			raw:  "ghes://org/repo/1/sha1",
			want: ChangeID{
				Scheme:        "ghes",
				Org:           "org",
				Repo:          "repo",
				PRNumber:      1,
				HeadCommitSHA: "sha1",
			},
		},
		{
			name: "nested org path",
			raw:  "github://uber/frontend/webapp/42/abc123",
			want: ChangeID{
				Scheme:        "github",
				Org:           "uber/frontend",
				Repo:          "webapp",
				PRNumber:      42,
				HeadCommitSHA: "abc123",
			},
		},
		{
			name:    "missing separator",
			raw:     "github/uber/submitqueue/123/abc123",
			wantErr: true,
		},
		{
			name:    "empty scheme",
			raw:     "://uber/submitqueue/123/abc123",
			wantErr: true,
		},
		{
			name:    "too few segments",
			raw:     "github://uber/123/abc123",
			wantErr: true,
		},
		{
			name:    "only one segment",
			raw:     "github://abc123",
			wantErr: true,
		},
		{
			name:    "empty owner",
			raw:     "github:///submitqueue/123/abc123",
			wantErr: true,
		},
		{
			name:    "empty repo",
			raw:     "github://uber//123/abc123",
			wantErr: true,
		},
		{
			name:    "non-numeric PR number",
			raw:     "github://uber/submitqueue/abc/abc123",
			wantErr: true,
		},
		{
			name:    "empty SHA",
			raw:     "github://uber/submitqueue/123/",
			wantErr: true,
		},
		{
			name:    "empty string",
			raw:     "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseChangeID(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestChangeID_String(t *testing.T) {
	tests := []struct {
		name string
		id   ChangeID
		want string
	}{
		{
			name: "github",
			id: ChangeID{
				Scheme:        "github",
				Org:           "uber",
				Repo:          "submitqueue",
				PRNumber:      123,
				HeadCommitSHA: "abc123",
			},
			want: "github://uber/submitqueue/123/abc123",
		},
		{
			name: "ghe",
			id: ChangeID{
				Scheme:        "ghe",
				Org:           "corp",
				Repo:          "app",
				PRNumber:      99,
				HeadCommitSHA: "deadbeef",
			},
			want: "ghe://corp/app/99/deadbeef",
		},
		{
			name: "ghes",
			id: ChangeID{
				Scheme:        "ghes",
				Org:           "org",
				Repo:          "repo",
				PRNumber:      1,
				HeadCommitSHA: "sha1",
			},
			want: "ghes://org/repo/1/sha1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.id.String())
		})
	}
}

func TestChangeID_OwnerRepo(t *testing.T) {
	id := ChangeID{
		Scheme:        "github",
		Org:           "uber",
		Repo:          "submitqueue",
		PRNumber:      1,
		HeadCommitSHA: "abc",
	}
	assert.Equal(t, "uber/submitqueue", id.OwnerRepo())
}

func TestParseChangeID_RoundTrip(t *testing.T) {
	originals := []string{
		"github://uber/submitqueue/123/abc123def456",
		"ghe://corp/monorepo/99/deadbeef01234567",
		"ghes://org/repo/1/a1b2c3",
	}

	for _, raw := range originals {
		t.Run(raw, func(t *testing.T) {
			parsed, err := ParseChangeID(raw)
			require.NoError(t, err)
			assert.Equal(t, raw, parsed.String())
		})
	}
}
