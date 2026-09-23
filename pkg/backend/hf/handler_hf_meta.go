package hf

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/matrixhub-ai/hfd/pkg/hfmeta"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

const (
	maxReadmeBytes = 2 << 20
	maxConfigBytes = 1 << 20
)

// repoMetadata holds the card metadata of one repository revision, read from README.md front
// matter and, for models, config.json. Both the list projections and the repo info endpoint use it.
type repoMetadata struct {
	card        *hfmeta.Card
	cardData    json.Marshaler
	tags        []string
	pipelineTag string
	libraryName string
	sdk         string
	description string
}

// collectRepoMetadata reads the metadata of repo at rev for a repository of repoType.
// Missing, oversized or malformed files contribute nothing rather than failing the caller.
func collectRepoMetadata(repo *repository.Repository, rev, repoType string) repoMetadata {
	var meta repoMetadata
	seen := map[string]bool{}
	addTags := func(tags ...string) {
		for _, tag := range tags {
			if tag != "" && !seen[tag] {
				seen[tag] = true
				meta.tags = append(meta.tags, tag)
			}
		}
	}
	if data, ok := readBlob(repo, rev, "README.md", maxReadmeBytes); ok {
		body, hasCard := readmeBody(data)
		if rm, err := hfmeta.ParseReadme(bytes.NewReader(data)); err == nil && hasCard {
			meta.card, meta.cardData = rm.Card, rm.CardData
			meta.pipelineTag, meta.libraryName, meta.sdk = rm.Card.PipelineTag, rm.Card.LibraryName, rm.Card.SDK
			meta.description, _ = rm.Card.Extra["description"].(string)
			addTags(cardTags(rm, repoType)...)
		}
		if meta.description == "" {
			meta.description = firstParagraph(body)
		}
	}
	if repoType == "models" {
		if data, ok := readBlob(repo, rev, "config.json", maxConfigBytes); ok {
			if cfg, err := hfmeta.ParseConfigData(bytes.NewReader(data)); err == nil {
				addTags(cfg.Tags()...)
			}
		}
	}
	return meta
}

// cardJSON returns the card as it is served, or nil when it cannot be encoded (NaN, for one).
func (m repoMetadata) cardJSON() json.RawMessage {
	if m.cardData == nil {
		return nil
	}
	raw, err := m.cardData.MarshalJSON()
	if err != nil {
		return nil
	}
	return json.RawMessage(raw)
}

// cardTags returns the Hub's tag set for a card of repoType: models get the readme tags plus
// dataset: entries, spaces add their SDK, and datasets prefix each field with its name.
func cardTags(rm *hfmeta.Readme, repoType string) []string {
	c := rm.Card
	switch repoType {
	case "models":
		tags := rm.Tags()
		for _, d := range c.Datasets {
			tags = append(tags, "dataset:"+d)
		}
		return tags
	case "spaces":
		return append(rm.Tags(), c.SDK)
	}
	var library []string
	if c.LibraryName != "" {
		library = []string{c.LibraryName}
	}
	var tags []string
	for _, g := range []struct {
		prefix string
		values []string
	}{
		{"task_categories:", c.TaskCategories}, {"task_ids:", c.TaskIDs}, {"annotations_creators:", c.AnnotationsCreators},
		{"language_creators:", c.LanguageCreators}, {"multilinguality:", c.Multilinguality}, {"source_datasets:", c.SourceDatasets},
		{"language:", c.Language}, {"license:", c.License}, {"size_categories:", c.SizeCategories},
		{"library:", library}, {"arxiv:", rm.ArxivIDs}, {"", c.Tags},
	} {
		for _, v := range g.values {
			if v != "" {
				tags = append(tags, g.prefix+v)
			}
		}
	}
	return tags
}

// readBlob returns the content of name at rev when it exists and is within limit bytes.
func readBlob(repo *repository.Repository, rev, name string, limit int64) ([]byte, bool) {
	blob, err := repo.Blob(rev, name)
	if err != nil || blob.Size() > limit {
		return nil, false
	}
	rc, err := blob.NewReader()
	if err != nil {
		return nil, false
	}
	defer rc.Close()
	data, err := io.ReadAll(io.LimitReader(rc, limit))
	return data, err == nil
}

// readmeBody splits off the front matter the way hfmeta detects it, so hasCard agrees with ParseReadme.
func readmeBody(data []byte) (body []byte, hasCard bool) {
	if !bytes.HasPrefix(data, []byte("---\n")) && !bytes.HasPrefix(data, []byte("---\r\n")) {
		return data, false
	}
	_, rest, _ := bytes.Cut(data, []byte("\n"))
	end := bytes.Index(rest, []byte("\n---"))
	if end < 0 {
		return data, false
	}
	_, body, _ = bytes.Cut(rest[end+1:], []byte("\n"))
	return body, true
}

// firstParagraph returns the first paragraph of body that is not only headings.
func firstParagraph(body []byte) string {
	for _, para := range strings.Split(strings.ReplaceAll(string(body), "\r\n", "\n"), "\n\n") {
		para = strings.TrimSpace(para)
		if para == "" {
			continue
		}
		if slices.ContainsFunc(strings.Split(para, "\n"), func(line string) bool { return !strings.HasPrefix(strings.TrimSpace(line), "#") }) {
			return para
		}
	}
	return ""
}

// repoSiblings lists the files of repo at rev as the Hub's siblings entries.
func repoSiblings(repo *repository.Repository, rev string) ([]sibling, error) {
	entries, err := repo.Tree(rev, "", &repository.TreeOptions{Recursive: true})
	if err != nil {
		return nil, err
	}
	siblings := []sibling{}
	for _, e := range entries {
		if e.Type() == repository.EntryTypeFile {
			siblings = append(siblings, sibling{RFilename: e.Path()})
		}
	}
	return siblings, nil
}

// repoDates are the tip commit of a revision and the dates the Hub derives from it.
type repoDates struct {
	sha                     string
	lastModified, createdAt time.Time
}

func hubTime(t time.Time) string {
	return t.UTC().Format(repository.TimeFormat)
}

type tipEntry struct {
	tip       repository.Hash
	createdAt time.Time
}

// tipCache remembers the earliest reachable author date per repository path, keyed to the tip it was
// computed from: Git has no creation time, so createdAt is that approximation.
type tipCache struct {
	mu      sync.Mutex
	entries map[string]tipEntry
}

const tipCacheSize = 256

// dates returns the tip of rev with its author date as lastModified and, when wantCreated, the
// earliest author date reachable from it. A revision without commits yields zero dates.
func (c *tipCache) dates(repo *repository.Repository, key, rev string, wantCreated bool) repoDates {
	tips, err := repo.Commits(rev, &repository.CommitsOptions{Limit: 1})
	if err != nil || len(tips) == 0 {
		return repoDates{}
	}
	tip := tips[0]
	d := repoDates{sha: tip.Hash().String(), lastModified: tip.Author().When()}
	if !wantCreated {
		return d
	}
	c.mu.Lock()
	e, ok := c.entries[key]
	c.mu.Unlock()
	if ok && e.tip == tip.Hash() {
		d.createdAt = e.createdAt
		return d
	}
	all, err := repo.Commits(rev, nil)
	if err != nil {
		return d
	}
	d.createdAt = tip.Author().When()
	for i := range all {
		if when := all[i].Author().When(); when.Before(d.createdAt) {
			d.createdAt = when
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= tipCacheSize {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}
	c.entries[key] = tipEntry{tip: tip.Hash(), createdAt: d.createdAt}
	return d
}
