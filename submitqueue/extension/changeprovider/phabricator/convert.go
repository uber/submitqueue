package phabricator

import (
	"strconv"

	"github.com/uber/submitqueue/submitqueue/entity"
	entityphab "github.com/uber/submitqueue/submitqueue/entity/phabricator"
)

// convertToChangeInfo converts a Phabricator diff result to an entity.ChangeInfo.
func convertToChangeInfo(parsed entityphab.ChangeID, diff *diffResult) entity.ChangeInfo {
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
// to zero. Unlike GitHub, Phabricator reports both additions and deletions, so
// LinesModified is set to their sum.
func convertFiles(changes []fileChange) []entity.ChangedFile {
	files := make([]entity.ChangedFile, 0, len(changes))
	for _, c := range changes {
		added, _ := strconv.Atoi(c.AddLines)
		deleted, _ := strconv.Atoi(c.DelLines)
		files = append(files, entity.ChangedFile{
			Path:          c.CurrentPath,
			LinesAdded:    added,
			LinesDeleted:  deleted,
			LinesModified: added + deleted,
		})
	}
	return files
}
