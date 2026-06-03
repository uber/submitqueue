package github

import (
	entitygithub "github.com/uber/submitqueue/submitqueue/entity/github"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
)

// convertToChangeInfo converts GitHub PR data to ChangeInfo.
func convertToChangeInfo(parsed entitygithub.ChangeID, prData *pullRequestData) changeprovider.ChangeInfo {
	changedFiles := convertFiles(prData.Files.Nodes)

	return changeprovider.ChangeInfo{
		URI: parsed.String(),
		User: changeprovider.User{
			Name:  prData.Author.Name,
			Email: prData.Author.Email,
		},
		ChangedFiles: changedFiles,
	}
}

// convertFiles converts GitHub file nodes to ChangedFile structs.
func convertFiles(nodes []fileNode) []changeprovider.ChangedFile {
	changedFiles := make([]changeprovider.ChangedFile, 0, len(nodes))

	for _, file := range nodes {
		changedFiles = append(changedFiles, changeprovider.ChangedFile{
			Path:         file.Path,
			Patch:        file.Patch,
			LinesAdded:   file.Additions,
			LinesDeleted: file.Deletions,
		})
	}

	return changedFiles
}
