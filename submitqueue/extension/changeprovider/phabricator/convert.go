package phabricator

import (
	"strconv"

	changephab "github.com/uber/submitqueue/platform/base/change/phabricator"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// convertToChangeInfo converts a Phabricator diff result to an entity.ChangeInfo.
func convertToChangeInfo(parsed changephab.ChangeID, diff *diffResult) entity.ChangeInfo {
	return entity.ChangeInfo{
		URI: parsed.String(),
		Details: entity.ChangeDetails{
			Author: entity.Author{
				Name:  diff.AuthorName,
				Email: diff.AuthorEmail,
			},
			ChangedFiles: convertFiles(diff.Changes),
		},
	}
}

// convertFiles converts Phabricator file changes to entity.ChangedFile structs.
// Phabricator reports addLines and delLines as strings; parsing failures default
// to zero. The Conduit API does not return a separate modified-lines count, so
// LinesModified is left at its zero value.
func convertFiles(changes []fileChange) []entity.ChangedFile {
	files := make([]entity.ChangedFile, 0, len(changes))
	for _, c := range changes {
		added, _ := strconv.Atoi(c.AddLines)
		deleted, _ := strconv.Atoi(c.DelLines)
		files = append(files, entity.ChangedFile{
			Path:         c.CurrentPath,
			LinesAdded:   added,
			LinesDeleted: deleted,
		})
	}
	return files
}
