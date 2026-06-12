package phabricator

import (
	"testing"

	"github.com/stretchr/testify/assert"

	entityphab "github.com/uber/submitqueue/entity/change/phabricator"
	"github.com/uber/submitqueue/submitqueue/entity"
)

func TestConvertToChangeInfo(t *testing.T) {
	parsed := entityphab.ChangeID{Scheme: "phab", RevisionID: 12345, DiffID: 100}
	diff := &diffResult{
		AuthorName:  "Alice",
		AuthorEmail: "alice@example.com",
		Changes: []fileChange{
			{CurrentPath: "main.go", AddLines: "10", DelLines: "3"},
			{CurrentPath: "test.go", AddLines: "20", DelLines: "0"},
		},
	}

	info := convertToChangeInfo(parsed, diff)

	assert.Equal(t, "phab://D12345/100", info.URI)
	assert.Equal(t, entity.Author{Name: "Alice", Email: "alice@example.com"}, info.Details.Author)
	assert.Len(t, info.Details.ChangedFiles, 2)
}

func TestConvertFiles(t *testing.T) {
	testCases := []struct {
		name     string
		changes  []fileChange
		expected []entity.ChangedFile
	}{
		{
			name: "normal files",
			changes: []fileChange{
				{CurrentPath: "a.go", AddLines: "10", DelLines: "5"},
				{CurrentPath: "b.go", AddLines: "0", DelLines: "3"},
			},
			expected: []entity.ChangedFile{
				{Path: "a.go", LinesAdded: 10, LinesDeleted: 5, LinesModified: 15},
				{Path: "b.go", LinesAdded: 0, LinesDeleted: 3, LinesModified: 3},
			},
		},
		{
			name:     "empty changes",
			changes:  []fileChange{},
			expected: []entity.ChangedFile{},
		},
		{
			name: "unparseable line counts default to zero",
			changes: []fileChange{
				{CurrentPath: "c.go", AddLines: "not_a_number", DelLines: ""},
			},
			expected: []entity.ChangedFile{
				{Path: "c.go", LinesAdded: 0, LinesDeleted: 0, LinesModified: 0},
			},
		},
		{
			name: "add-only file",
			changes: []fileChange{
				{CurrentPath: "new.go", AddLines: "52", DelLines: "0"},
			},
			expected: []entity.ChangedFile{
				{Path: "new.go", LinesAdded: 52, LinesDeleted: 0, LinesModified: 52},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := convertFiles(tc.changes)
			assert.Equal(t, tc.expected, result)
		})
	}
}
