package hfmeta

import (
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// Metadata is the card metadata of a repository revision, from README.md YAML front matter and config.json.
type Metadata struct {
	Tags        []string
	PipelineTag string
	LibraryName string
	CardData    any
}

// Collect reads the metadata of an already-opened repository at rev; unreadable files contribute nothing.
func Collect(repo *repository.Repository, rev string) Metadata {
	var meta Metadata

	seen := make(map[string]struct{})
	addTag := func(tag string) {
		if tag == "" {
			return
		}
		if _, ok := seen[tag]; !ok {
			seen[tag] = struct{}{}
			meta.Tags = append(meta.Tags, tag)
		}
	}

	// README.md YAML front matter
	if blob, err := repo.Blob(rev, "README.md"); err == nil {
		if rc, err := blob.NewReader(); err == nil {
			if rm, err := ParseReadme(rc); err == nil {
				for _, tag := range rm.Tags() {
					addTag(tag)
				}
				meta.PipelineTag = rm.Card.PipelineTag
				meta.LibraryName = rm.Card.LibraryName
				meta.CardData = rm.CardData
			}
			rc.Close()
		}
	}

	// config.json
	if blob, err := repo.Blob(rev, "config.json"); err == nil {
		if rc, err := blob.NewReader(); err == nil {
			if cfg, err := ParseConfigData(rc); err == nil {
				for _, tag := range cfg.Tags() {
					addTag(tag)
				}
			}
			rc.Close()
		}
	}

	return meta
}
