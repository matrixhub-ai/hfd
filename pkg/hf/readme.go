package hf

import (
	"encoding/json"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

type Readme struct {
	Card *Card

	CardData json.Marshaler

	// ArxivIDs holds ArXiv paper identifiers extracted from links in the README body
	// (not from the YAML front matter). Each value is a bare ID like "2310.06825".
	// Populated by ParseCardData when the README body contains arxiv.org links.
	ArxivIDs []string
}

type lazyCardData struct {
	yamlData []byte
	jsonData []byte
}

func (lcd *lazyCardData) MarshalJSON() ([]byte, error) {
	if lcd.jsonData != nil {
		return lcd.jsonData, nil
	}
	if lcd.yamlData == nil {
		return []byte("{}"), nil
	}
	jsonData, err := yamlToJSON(lcd.yamlData)
	if err != nil {
		return nil, err
	}
	lcd.jsonData = jsonData
	return jsonData, nil
}

// ParseReadme reads a README.md and extracts metadata from its YAML front matter.
// It also scans the body (the part after the front matter) for arXiv paper links,
// which are stored in Readme.ArxivIDs.
//
// If the file does not start with a "---" front matter block, or the block has no
// fields that match CardData, a zero-value *CardData is returned with no error.
// Parse errors in the YAML block are returned as errors.
func ParseReadme(r io.Reader) (*Readme, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	fm, body := extractFrontMatterAndBody(data)

	var card Card
	if fm != nil {
		if err := yaml.Unmarshal(fm, &card); err != nil {
			return nil, fmt.Errorf("failed to parse card front matter: %w", err)
		}
	}

	readme := &Readme{
		Card:     &card,
		CardData: &lazyCardData{yamlData: fm},
		ArxivIDs: extractArxivIDs(body),
	}
	return readme, nil
}

// Tags returns the full computed set of tags for this card, matching the tags
// that the HuggingFace Hub API returns in the top-level "tags" field of repo info.
//
// Sources merged in order (duplicates are silently dropped):
//  1. Explicit tags from the "tags" YAML field
//  2. Language codes from the "language" field (e.g. "en", "fr")
//  3. License values from the "license" field, prefixed with "license:" (e.g. "license:mit")
//  4. The "pipeline_tag" value (e.g. "text-generation")
//  5. The "library_name" value (e.g. "transformers")
//  6. ArXiv IDs found in the README body, prefixed with "arxiv:" (e.g. "arxiv:2310.06825")
//  7. "eval-results" when the model-index block is present
func (c *Readme) Tags() []string {
	seen := make(map[string]struct{})
	var tags []string

	add := func(tag string) {
		if tag == "" {
			return
		}
		if _, ok := seen[tag]; !ok {
			seen[tag] = struct{}{}
			tags = append(tags, tag)
		}
	}

	for _, tag := range c.Card.Tags {
		add(tag)
	}
	for _, lang := range c.Card.Language {
		add(lang)
	}
	for _, lic := range c.Card.License {
		add("license:" + lic)
	}
	add(c.Card.PipelineTag)
	add(c.Card.LibraryName)
	for _, arxivID := range c.ArxivIDs {
		add("arxiv:" + arxivID)
	}
	if len(c.Card.ModelIndex) > 0 {
		add("eval-results")
	}

	return tags
}

func yamlToJSON(yamlData []byte) ([]byte, error) {
	var data any
	if err := yaml.Unmarshal(yamlData, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}
	return jsonData, nil
}
