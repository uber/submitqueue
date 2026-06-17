package github

import (
	entitygithub "github.com/uber/submitqueue/platform/base/change/github"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// convertToChangeInfo converts GitHub PR data to an entity.ChangeInfo.
func convertToChangeInfo(parsed entitygithub.ChangeID, prData *pullRequestData) entity.ChangeInfo {
	return entity.ChangeInfo{
		URI: parsed.String(),
		Details: entity.ChangeDetails{
			Author: entity.Author{
				Name:  prData.Author.Name,
				Email: prData.Author.Email,
			},
			ChangedFiles: convertFiles(prData.Files.Nodes),
		},
	}
}

// convertFiles converts GitHub file nodes to entity.ChangedFile structs.
// GitHub's API reports only additions and deletions per file, so LinesModified
// is left zero here.
func convertFiles(nodes []fileNode) []entity.ChangedFile {
	changedFiles := make([]entity.ChangedFile, 0, len(nodes))

	for _, file := range nodes {
		changedFiles = append(changedFiles, entity.ChangedFile{
			Path:         file.Path,
			LinesAdded:   file.Additions,
			LinesDeleted: file.Deletions,
		})
	}

	return changedFiles
}
